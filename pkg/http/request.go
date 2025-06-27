package http

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

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
	databaseKey      *auth.DatabaseKey
	databaseManager  *database.DatabaseManager
	logManager       *logs.LogManager
	cluster          *cluster.Cluster
	headers          Headers
	Method           string
	QueryParams      map[string]string
	requestToken     auth.RequestToken
	Route            Route
}

// Create a new Request instance.
func NewRequest(
	cluster *cluster.Cluster,
	databaseManager *database.DatabaseManager,
	logManager *logs.LogManager,
	request *http.Request,
) *Request {
	headers := make(map[string]string, len(request.Header))

	for key, value := range request.Header {
		headers[key] = value[0]
	}

	headers["host"] = request.Host

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
		headers:          NewHeaders(headers),
		logManager:       logManager,
		Method:           request.Method,
		QueryParams:      queryParams,
	}
}

// Return all of the data from the request body as a map.
func (r *Request) All() map[string]any {
	if r.Body == nil &&
		r.BaseRequest.Body != nil &&
		r.Headers().Get("Content-Type") == "application/json" &&
		r.Headers().Get("Content-Length") != "0" {
		// Read the raw body bytes first for hashing
		rawBody, err := io.ReadAll(r.BaseRequest.Body)

		if err != nil {
			slog.Error("error reading request body", "error", err)
			return nil
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
	username, password, ok := r.BaseRequest.BasicAuth()

	if ok {
		if !r.cluster.Auth.UserManager().Authenticate(username, password) {
			return fmt.Errorf("invalid username or password")
		}

		if r.cluster.Auth.UserManager().Get(username).AuthorizeForResource(
			resources,
			actions,
		) {
			return nil
		}

		return fmt.Errorf("user is not authorized to perform this request")
	}

	accessKey := r.RequestToken("Authorization").AccessKey()

	if accessKey == nil {
		return fmt.Errorf("invalid access key")
	}

	if !accessKey.AuthorizeForResource(
		resources,
		actions,
	) {
		return fmt.Errorf("access key is not authorized to perform this request")
	}

	return nil
}

// Return a database key for this request.
func (r *Request) DatabaseKey() *auth.DatabaseKey {
	if r.databaseKey != nil {
		return r.databaseKey
	}

	// Get the database key from the subdomain
	key := r.Param("databaseKey")

	if key == "" {
		return nil
	}

	databaseKey, err := r.databaseManager.GetKey(key)

	if err != nil {
		return nil
	}

	r.databaseKey = auth.NewDatabaseKey(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
		databaseKey.Key,
	)

	return r.databaseKey
}

// Get a value from the request body by its key.
func (r *Request) Get(key string) any {
	return r.All()[key]
}

// Return the headers for the request.
func (request *Request) Headers() Headers {
	return request.headers
}

// Map the request body to a struct of the type input.
func (request *Request) Input(input any) (any, error) {
	jsonData, err := json.Marshal(request.All())

	if err != nil {
		return nil, err
	}

	if string(jsonData) == "null" || len(jsonData) == 0 {
		return nil, fmt.Errorf("request body is empty")
	}

	err = json.Unmarshal(jsonData, &input)

	if err != nil {
		return nil, err
	}

	return input, nil
}

// Load the database key if it is not already loaded.
func (request *Request) loadDatabaseKey() {
	if request.databaseKey == nil {
		go request.DatabaseKey()
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
	value := request.QueryParams[key]

	if value == "" && len(defaultValue) > 0 {
		return defaultValue[0]
	}

	return value
}

// Return the request token for this request.
func (request *Request) RequestToken(header string) auth.RequestToken {
	if !request.requestToken.Valid() {
		request.requestToken = auth.CaptureRequestToken(
			request.accessKeyManager,
			request.headers.Get(header),
		)
	}

	return request.requestToken
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
