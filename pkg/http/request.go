package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/litebase/litebase/internal/validation"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/server/logs"
)

type Request struct {
	accessKeyManager *auth.AccessKeyManager
	BaseRequest      *http.Request
	Body             map[string]any
	databaseKey      *auth.DatabaseKey
	databaseManager  *database.DatabaseManager
	logManager       *logs.LogManager
	cluster          *cluster.Cluster
	headers          Headers
	Method           string
	QueryParams      map[string]string
	requestToken     auth.RequestToken
	Route            Route
	subdomains       []string
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

	// Parse the subdomains above the root domain name once
	domainName := cluster.Config.DomainName
	host := strings.Replace(request.Host, domainName, "", 1)
	parts := strings.Split(host, ".")

	var subdomains []string

	if len(parts) >= 4 {
		subdomains = parts[0:3]
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
		subdomains:       subdomains,
	}
}

// Return all of the data from the request body as a map.
func (r *Request) All() map[string]any {
	if r.Body == nil && r.BaseRequest.Body != nil {
		body := make(map[string]any)
		json.NewDecoder(r.BaseRequest.Body).Decode(&body)
		r.BaseRequest.Body.Close()

		r.Body = body
	}

	return r.Body
}

// Authorize the request based on the access key and the specified resource and actions.
func (r *Request) Authorize(resources []string, actions []auth.Privilege) error {
	username, password, ok := r.BaseRequest.BasicAuth()

	if ok {
		if !r.cluster.Auth.UserManager().Authenticate(username, password) {
			return fmt.Errorf("unauthorized: invalid username or password")
		}

		if r.cluster.Auth.UserManager().Get(username).AuthorizeForResource(
			resources,
			actions,
		) {
			return nil
		}

		return fmt.Errorf("unauthorized: user is not authorized to perform this request")
	}

	if len(r.Subdomains()) == 0 {
		return fmt.Errorf("unauthorized")
	}

	accessKey := r.RequestToken("Authorization").AccessKey(r.Subdomains()[0])

	if accessKey == nil {
		return fmt.Errorf("unauthorized: invalid access key")
	}

	if !accessKey.AuthorizeForResource(
		resources,
		actions,
	) {
		return fmt.Errorf("unauthorized: access key is not authorized to perform this request")
	}

	return nil
}

// Return the cluster id for this request.
func (r *Request) ClusterId() string {
	subdomains := r.Subdomains()

	return subdomains[1]
}

// Return a database key for this request.
func (r *Request) DatabaseKey() *auth.DatabaseKey {
	if r.databaseKey != nil {
		return r.databaseKey
	}

	if len(r.Subdomains()) == 0 {
		return nil
	}

	// Get the database key from the subdomain
	key := r.Subdomains()[0]

	if key == "" || len(r.Subdomains()) != 3 {
		log.Println("subdomain is not valid:", r.Subdomains())
		return nil
	}

	databaseKey, err := r.cluster.Auth.SecretsManager.GetDatabaseKey(
		key,
	)

	if err != nil {
		log.Println("error getting database key:", err)
		return nil
	}

	r.databaseKey = databaseKey

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

// Return the region of the request parsed from the subdomains.
func (request *Request) Region() string {
	subdomains := request.Subdomains()

	return subdomains[2]
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

// Return the subdomains for this request.
func (request *Request) Subdomains() []string {
	return request.subdomains
}

func (request *Request) Validate(
	input any,
	messages map[string]string,
) map[string][]string {
	if err := validation.Validate(input); err != nil {
		var e map[string][]string = make(map[string][]string)

		for _, x := range err {
			fieldKey := x.Field()
			namespace := x.Namespace()
			tag := x.Tag()

			// Check if this is a slice dive validation error
			if namespace != "" && strings.Contains(namespace, "[") && strings.Contains(namespace, "]") {
				// Remove the first part of the namespace which is the struct name
				// e.g. "TestStruct.users[0].email" -> "users[0].email"
				namespace = namespace[strings.Index(namespace, ".")+1:]

				// Convert array index notation to wildcard for message lookup
				wildcardKey := strings.ReplaceAll(namespace, "[", ".")
				wildcardKey = strings.ReplaceAll(wildcardKey, "]", "")

				// Replace numeric indices with wildcards
				parts := strings.Split(wildcardKey, ".")
				partNumbers := []int{}

				for i, part := range parts {
					if number, err := strconv.Atoi(part); err == nil {
						parts[i] = "*"
						partNumbers = append(partNumbers, number)
						break
					}
				}

				wildcardKey = strings.Join(parts, ".")
				messageKey := fmt.Sprintf("%s.%s", wildcardKey, tag)

				if messages[messageKey] == "" {
					continue
				}

				var result strings.Builder
				numIdx := 0

				for _, ch := range wildcardKey {
					if ch == '*' && numIdx < len(partNumbers) {
						result.WriteString(strconv.Itoa(partNumbers[numIdx]))
						numIdx++
					} else {
						result.WriteRune(ch)
					}
				}

				errorKey := result.String()

				e[errorKey] = append(e[errorKey], messages[messageKey])
			} else {
				messageKey := fmt.Sprintf("%s.%s", fieldKey, tag)
				e[fieldKey] = append(e[fieldKey], messages[messageKey])
			}
		}

		return e
	}

	return nil
}
