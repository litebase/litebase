package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/litebase/litebase/internal/validation"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/database"
	"github.com/litebase/litebase/server/logs"
)

type Request struct {
	accessKeyManager *auth.AccessKeyManager
	BaseRequest      *http.Request
	Body             map[string]any
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

func (r *Request) All() map[string]any {
	if r.Body == nil {
		body := make(map[string]any)
		json.NewDecoder(r.BaseRequest.Body).Decode(&body)
		r.BaseRequest.Body.Close()

		r.Body = body
	}

	return r.Body
}

func (r *Request) ClusterId() string {
	subdomains := r.Subdomains()

	return subdomains[1]
}

func (r *Request) DatabaseKey() *auth.DatabaseKey {
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

	return databaseKey
}

func (r *Request) Get(key string) any {
	return r.All()[key]
}

func (request *Request) Headers() Headers {
	return request.headers
}

func (request *Request) Input(input any) (any, error) {
	jsonData, err := json.Marshal(request.All())

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(jsonData, &input)

	if err != nil {
		return nil, err
	}

	return input, nil
}

func (request *Request) Param(key string) string {
	return request.BaseRequest.PathValue(key)
}

func (request *Request) Path() string {
	return request.BaseRequest.URL.Path
}

func (request *Request) QueryParam(key string, defaultValue ...string) string {
	value := request.QueryParams[key]

	if value == "" && len(defaultValue) > 0 {
		return defaultValue[0]
	}

	return value
}

func (request *Request) Region() string {
	subdomains := request.Subdomains()

	return subdomains[2]
}

func (request *Request) RequestToken(header string) auth.RequestToken {
	if !request.requestToken.Valid() {
		request.requestToken = auth.CaptureRequestToken(
			request.accessKeyManager,
			request.headers.Get(header),
		)
	}

	return request.requestToken
}

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
			if e[x.Field()] == nil {
				e[x.Field()] = []string{}
			}

			key := fmt.Sprintf("%s.%s", x.Field(), x.Tag())

			if messages[key] == "" {
				continue
			}

			e[x.Field()] = append(e[x.Field()], messages[key])
		}

		return e
	}

	return nil
}
