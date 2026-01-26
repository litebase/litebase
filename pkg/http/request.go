package http

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/litebase/litebase/internal/validation"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/logs"
)

type Request struct {
	accessKeyManager *auth.AccessKeyManager
	BaseRequest      *http.Request
	Body             map[string]any
	bodyHash         string
	credential       *auth.Credential
	databaseKey      *auth.DatabaseKey
	databaseKeyMutex sync.RWMutex
	databaseManager  *database.DatabaseManager
	logManager       *logs.LogManager
	cluster          *cluster.Cluster
	headers          *Headers
	Method           string
	queryParams      map[string]string
	Route            Route
}

// ParamError represents a validation/conversion error for a single query parameter.
type ParamError struct {
	Field   string // JSON name of the field
	Message string // Human friendly message (without the "Error: " prefix)
}

func (e ParamError) Error() string {
	return fmt.Sprintf("Error: %s", e.Message)
}

// Create a new Request instance.
func NewRequest(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	request *http.Request,
) *Request {
	queryParams := make(map[string]string, len(request.URL.Query()))

	for key, value := range request.URL.Query() {
		queryParams[key] = value[0]
	}

	return &Request{
		accessKeyManager: cluster.Auth.AccessKeyManager,
		BaseRequest:      request,
		Body:             nil,
		cluster:          cluster,
		databaseManager:  databaseManager,
		logManager:       logManager,
		Method:           request.Method,
		queryParams:      queryParams,
	}
}

// Return all of the data from the request body as a map.
func (r *Request) All() map[string]any {
	if r.Body == nil &&
		r.BaseRequest != nil &&
		r.BaseRequest.Body != nil &&
		r.Headers().Get("Content-Type") == "application/json" &&
		r.Headers().Get("Content-Length") != "0" {
		// Read the raw body bytes first for hashing
		rawBody, err := io.ReadAll(r.BaseRequest.Body)

		if err != nil {
			slog.Error("error reading request body", "error", err)
			return nil
		}

		if len(rawBody) == 0 {
			rawBody = []byte("")
		}

		// Calculate hash of the raw body
		bodyHashSum := sha256.Sum256(rawBody)
		r.bodyHash = fmt.Sprintf("%x", bodyHashSum)

		// Parse the body into a map
		body := make(map[string]any)

		if len(rawBody) > 0 {
			err := json.Unmarshal(rawBody, &body)

			if err != nil {
				slog.Error("error decoding request body", "error", err)
				return nil
			}
		}

		err = r.BaseRequest.Body.Close()

		if err != nil {
			slog.Error("error closing request body", "error", err)
		}

		r.Body = body
	}

	return r.Body
}

// Return the SHA256 hash of the request body that was calculated when All() was called.
func (r *Request) BodyHash() string {
	// Ensure All() has been called to populate the body hash
	if r.bodyHash == "" && r.BaseRequest.Body != nil {
		r.All()
	}

	return r.bodyHash
}

// Authorize the request based on the access key and the specified resource and actions.
func (r *Request) Authorize(resources []string, actions []auth.Privilege) error {
	credential := r.Credential()

	if credential.Invalid() {
		return errors.New("invalid request credential")
	}

	switch credential.Type() {
	case auth.CredentialTypeBasicAuth:
		username, _, ok := r.BaseRequest.BasicAuth()

		if ok {
			user, err := r.cluster.Auth.UserManager.Get(username)

			if err != nil {
				return fmt.Errorf("failed to get user: %w", err)
			}

			if user.AuthorizeForResource(resources, actions) {
				return nil
			}

			return fmt.Errorf("user is not authorized to perform this request")
		}
	case auth.CredentialTypeToken:
		token := r.Credential().Token()

		if token == nil {
			return fmt.Errorf("invalid token")
		}

		if !token.AuthorizeForResource(
			resources,
			actions,
		) {
			return fmt.Errorf("token is not authorized to perform this request")
		}

	case auth.CredentialTypeAccessKey:
		accessKey := r.Credential().AccessKey()

		if accessKey == nil {
			return fmt.Errorf("invalid access key")
		}

		if !accessKey.AuthorizeForResource(
			resources,
			actions,
		) {
			return fmt.Errorf("access key is not authorized to perform this request")
		}
	default:
		return errors.New("unable to authorize the request")
	}

	return nil
}

// Return the authentication credential for this request.
func (request *Request) Credential() *auth.Credential {
	if request.credential == nil {
		request.credential = auth.CaptureCredential(
			request.cluster.Auth,
			request.Headers().Get("Authorization"),
		)
	}

	return request.credential
}

// Return a database key for this request.
func (r *Request) DatabaseKey() (*auth.DatabaseKey, Response) {
	r.databaseKeyMutex.RLock()
	if r.databaseKey != nil {
		defer r.databaseKeyMutex.RUnlock()
		return r.databaseKey, Response{}
	}
	r.databaseKeyMutex.RUnlock()

	r.databaseKeyMutex.Lock()
	defer r.databaseKeyMutex.Unlock()

	// Double-check pattern: another goroutine might have set it while we waited for the lock
	if r.databaseKey != nil {
		return r.databaseKey, Response{}
	}

	databaseName := r.Param("databaseName")

	if databaseName == "" {
		return nil, ErrValidDatabaseNameRequiredResponse
	}

	branchName := r.Param("branchName")

	if branchName == "" {
		return nil, ErrValidBranchNameRequiredResponse
	}

	db, err := r.databaseManager.GetByName(databaseName)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundResponse(errors.New("database not found"))
		}

		return nil, BadRequestResponse(err)
	}

	branch, err := db.Branch(branchName)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, NotFoundResponse(errors.New("branch not found"))
		}

		return nil, BadRequestResponse(err)
	}

	r.databaseKey = auth.NewDatabaseKey(
		db.DatabaseID,
		db.Name,
		branch.DatabaseBranchID,
		branch.Name,
	)

	return r.databaseKey, Response{}
}

// Get a value from the request body by its key.
func (r *Request) Get(key string) any {
	return r.All()[key]
}

// Return the headers for the request.
func (request *Request) Headers() *Headers {
	if request.headers == nil {
		headers := make(map[string]string, len(request.BaseRequest.Header))

		for key, value := range request.BaseRequest.Header {
			headers[key] = value[0]
		}

		headers["host"] = request.BaseRequest.Host

		request.headers = NewHeaders(headers)
	}

	return request.headers
}

// Map the request body to a struct of the type input.
func (request *Request) Input(input any) (any, error) {
	jsonData, err := json.Marshal(request.All())

	if err != nil {
		return nil, err
	}

	if input == nil {
		return nil, errors.New("input cannot be nil")
	}

	err = json.Unmarshal(jsonData, &input)

	if err != nil {
		return nil, err
	}

	return input, nil
}

// Load the database key if it is not already loaded.
func (request *Request) loadDatabaseKey() {
	request.databaseKeyMutex.RLock()
	if request.databaseKey == nil {
		request.databaseKeyMutex.RUnlock()
		go request.DatabaseKey()
	} else {
		request.databaseKeyMutex.RUnlock()
	}
}

// Return a route parameter for the request by its key.
func (request *Request) Param(key string) string {
	return request.BaseRequest.PathValue(key)
}

// Return the path of the request.
func (request *Request) Path() string {
	return request.BaseRequest.URL.Path
}

// Return a query parameter from the request by its key.
func (request *Request) QueryParam(key string, defaultValue ...string) string {
	value := request.queryParams[key]

	if value == "" && len(defaultValue) > 0 {
		return defaultValue[0]
	}

	return value
}

// Map the query parameters to a struct of the type queryParamStruct.
func (request *Request) QueryParams(queryParamStruct any) (any, error) {
	// Prepare a map[string]any that will contain typed values (not only strings)
	// so that unmarshalling into the target struct preserves numeric/bool types
	// when the struct tags don't expect JSON strings.
	params := make(map[string]any)

	// Copy existing query params as raw strings first
	for k, v := range request.queryParams {
		params[k] = v
	}

	// We want to inspect the target struct's fields to fill defaults and
	// convert values to the appropriate types prior to JSON unmarshalling.
	rv := reflect.ValueOf(queryParamStruct)

	if rv.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("QueryParams requires a pointer to a struct")
	}

	rv = rv.Elem()
	rt := rv.Type()

	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("QueryParams requires a pointer to a struct")
	}

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		// Skip unexported fields
		if field.PkgPath != "" {
			continue
		}

		// Determine JSON name
		jsonTag := field.Tag.Get("json")
		jsonName := ""
		encodeAsString := false

		if jsonTag != "" {
			parts := strings.Split(jsonTag, ",")

			if parts[0] != "" {
				jsonName = parts[0]
			}

			for _, p := range parts[1:] {
				if p == "string" {
					encodeAsString = true
				}
			}
		}

		if jsonName == "" {
			jsonName = field.Name
		}

		// Default value from tag
		defaultVal := field.Tag.Get("default")

		// Check if a value is present
		rawVal, has := request.queryParams[jsonName]

		if !has || rawVal == "" {
			if defaultVal != "" {
				rawVal = defaultVal
				// Place default into params map as string for now; we'll convert below
				params[jsonName] = rawVal
				has = true
			} else {
				// nothing to do
				continue
			}
		}

		// Convert rawVal (string) into appropriate typed value in params map,
		// taking into account the field type and whether it uses `,string`.
		if !has {
			continue
		}

		// If the field is expected to be encoded as a JSON string (",string"),
		// leave it as string so encoding/json will decode it properly into
		// numeric fields that declare the ,string option.
		if encodeAsString {
			params[jsonName] = rawVal
			continue
		}

		// Otherwise attempt to convert to native types where sensible.
		kind := field.Type.Kind()

		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if iv, err := strconv.ParseInt(rawVal, 10, 64); err == nil {
				params[jsonName] = iv
			} else {
				// Try to provide a specific error message if possible
				errMsg := field.Tag.Get("error")

				if errMsg == "" {
					// Heuristics for common parameter names
					switch strings.ToLower(jsonName) {
					case "start":
						errMsg = "invalid start timestamp"
					case "end":
						errMsg = "invalid end timestamp"
					case "step":
						errMsg = "invalid step value"
					default:
						errMsg = fmt.Sprintf("invalid %s", jsonName)
					}
				}

				return nil, ParamError{Field: jsonName, Message: errMsg}
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if uv, err := strconv.ParseUint(rawVal, 10, 64); err == nil {
				params[jsonName] = uv
			} else {
				errMsg := field.Tag.Get("error")
				if errMsg == "" {
					switch strings.ToLower(jsonName) {
					case "start":
						errMsg = "invalid start timestamp"
					case "end":
						errMsg = "invalid end timestamp"
					default:
						errMsg = fmt.Sprintf("invalid %s", jsonName)
					}
				}

				return nil, ParamError{Field: jsonName, Message: errMsg}
			}
		case reflect.Float32, reflect.Float64:
			if fv, err := strconv.ParseFloat(rawVal, 64); err == nil {
				params[jsonName] = fv
			} else {
				errMsg := field.Tag.Get("error")
				if errMsg == "" {
					errMsg = fmt.Sprintf("invalid %s", jsonName)
				}

				return nil, ParamError{Field: jsonName, Message: errMsg}
			}
		case reflect.Bool:
			if bv, err := strconv.ParseBool(rawVal); err == nil {
				params[jsonName] = bv
			} else {
				errMsg := field.Tag.Get("error")
				if errMsg == "" {
					errMsg = fmt.Sprintf("invalid %s", jsonName)
				}

				return nil, ParamError{Field: jsonName, Message: errMsg}
			}
		case reflect.Slice:
			// For slices, keep the raw string. Advanced parsing (comma-separated)
			// could be added later.
			params[jsonName] = rawVal
		default:
			// string, struct, map, etc.: keep as string and let json.Unmarshal
			// handle the conversion where possible (e.g., nested Input)
			params[jsonName] = rawVal
		}
	}

	// Marshal the typed params map to JSON and unmarshal into the struct
	jsonData, err := json.Marshal(params)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonData, &queryParamStruct)

	if err != nil {
		return nil, err
	}

	return queryParamStruct, nil
}

// GetQueryLog returns a query log for the specified database, automatically
// configuring encryption if the database/branch is encrypted.
func (request *Request) GetQueryLog(databaseHash, databaseID, branchID string) (*logs.QueryLog, error) {
	queryLog := request.logManager.GetQueryLog(request.cluster, databaseHash, databaseID, branchID)

	// Check if the database is encrypted and configure encryption if needed
	db, err := request.databaseManager.Get(databaseID)

	if err != nil {
		return queryLog, nil // Return query log even if we can't check encryption
	}

	branch, err := db.BranchByID(branchID)

	if err != nil {
		return queryLog, nil // Return query log even if we can't check encryption
	}

	// If the branch is encrypted and query log isn't already encrypted, configure it
	if branch.Settings != nil && branch.Settings.Encrypted && branch.Settings.DataEncryptionKeyHash != "" && !queryLog.IsEncrypted() {
		dataKey, keyHash, err := database.MatchEncryptionKey(
			request.cluster.Config,
			branch.Settings.DataEncryptionKeyHash)

		if err != nil {
			slog.Error("Failed to match encryption key for query log", "error", err)
			return queryLog, err
		}

		err = queryLog.ConfigureEncryption(dataKey, keyHash)

		if err != nil {
			slog.Error("Failed to configure query log encryption", "error", err)
			return queryLog, err
		}
	}

	return queryLog, nil
}

// GetErrorLog returns an error log for the specified database, automatically
// configuring encryption if the database/branch is encrypted.
func (request *Request) GetErrorLog(databaseHash, databaseID, branchID string) (*logs.ErrorLog, error) {
	errorLog := request.logManager.GetErrorLog(request.cluster, databaseHash, databaseID, branchID)

	// Check if the database is encrypted and configure encryption if needed
	db, err := request.databaseManager.Get(databaseID)

	if err != nil {
		return errorLog, nil // Return error log even if we can't check encryption
	}

	branch, err := db.BranchByID(branchID)

	if err != nil {
		return errorLog, nil // Return error log even if we can't check encryption
	}

	// If the branch is encrypted and error log isn't already encrypted, configure it
	if branch.Settings != nil && branch.Settings.Encrypted && branch.Settings.DataEncryptionKeyHash != "" && !errorLog.IsEncrypted() {
		dataKey, keyHash, err := database.MatchEncryptionKey(
			request.cluster.Config,
			branch.Settings.DataEncryptionKeyHash)

		if err != nil {
			slog.Error("Failed to match encryption key for error log", "error", err)
			return errorLog, err
		}

		err = errorLog.ConfigureEncryption(dataKey, keyHash)

		if err != nil {
			slog.Error("Failed to configure error log encryption", "error", err)
			return errorLog, err
		}
	}

	return errorLog, nil
}

func (request *Request) Validate(
	input any,
	messages map[string]string,
) map[string][]string {
	if err := validation.Validate(input, messages); err != nil {
		return err
	}

	return nil
}
