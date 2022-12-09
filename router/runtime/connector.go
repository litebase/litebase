package runtime

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"litebasedb/router/auth"
	"litebasedb/router/config"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/gofiber/fiber/v2"
)

var Clients = make(map[string]*lambda.Lambda)

type RuntimeRequest struct {
	BranchUuid   string                 `json:"branchUuid"`
	DatabaseUuid string                 `json:"databaseUuid"`
	Body         map[string]string      `json:"body"`
	Method       string                 `json:"method"`
	Path         string                 `json:"path"`
	Server       map[string]interface{} `json:"server"`
}

type RuntimeResponse struct {
	StatusCode int                    `json:"statusCode"`
	Body       map[string]interface{} `json:"body"`
}

type RuntimeRequestObject struct {
	AccessKeyId  string
	Body         map[string]string
	BranchUuid   string
	DatabaseUuid string
	Headers      map[string]string
	Method       string
	Path         string
	Query        map[string]string
}

func ClientKey(databaseUuid, branchUuid string) string {
	return fmt.Sprintf("%s-%s", databaseUuid, branchUuid)
}

func CreateConnection(databaseUuid, branchUuid, fn string) bool {
	client := lambdaClient(databaseUuid, branchUuid)

	if client == nil {
		return false
	}

	request := prepareRequest(&RuntimeRequestObject{
		AccessKeyId:  "",
		Body:         nil,
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		Headers: map[string]string{
			"Content-Type": "application/json",
			"Host":         "localhost",
			"X-Lbdb-Date":  fmt.Sprintf("%x", time.Now().UTC().Unix()),
		},
		Method: "POST",
		Path:   "/connection",
		Query:  map[string]string{},
	}, true)

	payload, err := json.Marshal(request)

	if err != nil {
		return false
	}

	_, err = client.Invoke(&lambda.InvokeInput{
		FunctionName:   aws.String(fn),
		InvocationType: aws.String("Event"),
		Payload:        payload,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == lambda.ErrCodeServiceException {
				log.Println(lambda.ErrCodeServiceException, aerr.Error())

				Clients[ClientKey(databaseUuid, branchUuid)] = nil
			}
		}

		return false
	}

	return true
}

func ForwardRequest(request *fiber.Ctx, databaseUuid string, branchUuid string, accessKeyId string, fn string) *RuntimeResponse {
	var err error

	if fn == "" {
		fn, err = auth.SecretsManager().GetFunctionName(databaseUuid, branchUuid)

		if fn == "" || err != nil {
			log.Fatal(errors.New("this database is not properly configured"))
		}
	}

	if config.Get("env") == "local" {
		fn = "function"
	}

	body := make(map[string]string)
	request.BodyParser(&body)
	query := make(map[string]string)
	request.QueryParser(&query)

	preparedRequest := prepareRequest(&RuntimeRequestObject{
		AccessKeyId:  accessKeyId,
		Body:         body,
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		Headers:      request.GetReqHeaders(),
		Method:       request.Method(),
		Path:         request.OriginalURL(),
		Query:        query,
	}, accessKeyId == "")

	payload, err := json.Marshal(preparedRequest)

	if err != nil {
		return nil
	}

	startTime := time.Now()

	// var socketResult = false

	// fufillWithLambdaSocket :=

	executionContext := 1

	// if socketResult != false {
	// 	executionContext = 2
	// }

	result := FufillWithLambdaApi(databaseUuid, branchUuid, fn, payload)

	return PrepareResponse(
		startTime,
		executionContext,
		result,
	)
}

func FufillWithLambdaApi(databaseUuid string, branchUuid string, fn string, payload []byte) []byte {
	client := lambdaClient(databaseUuid, branchUuid)

	if client == nil {
		return nil
	}

	response, err := client.Invoke(&lambda.InvokeInput{
		FunctionName:   aws.String(fn),
		InvocationType: aws.String("RequestResponse"),
		Payload:        payload,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == lambda.ErrCodeServiceException {
				log.Println(lambda.ErrCodeServiceException, aerr.Error())

				Clients[ClientKey(databaseUuid, branchUuid)] = nil
			}
		}

		return nil
	}

	if aws.Int64Value(response.StatusCode) >= int64(400) {
		return nil
	}

	return response.Payload
}

func GetCredentials(datbaseUuid, branchUuid string) *auth.AWSCredentials {
	credentials, err := auth.SecretsManager().GetAwsCredentials(datbaseUuid, branchUuid)

	if err != nil {
		return nil
	}

	return credentials
}

func lambdaClient(databaseUuid, branchUuid string) *lambda.Lambda {
	clientKey := ClientKey(databaseUuid, branchUuid)

	if _, ok := Clients[clientKey]; !ok {
		awsCredentials := GetCredentials(databaseUuid, branchUuid)

		if awsCredentials == nil {
			return nil
		}

		var endpoint string

		if config.Get("env") == "local" {
			endpoint = "http://127.0.0.1:8001"
		}

		awsSession, err := session.NewSession(aws.NewConfig().WithRegion(config.Get("region")).WithCredentials(credentials.NewStaticCredentials(
			awsCredentials.Key,
			awsCredentials.Secret,
			awsCredentials.Token,
		)).WithEndpoint(endpoint))

		if err != nil {
			log.Fatal(err)
		}

		Clients[clientKey] = lambda.New(awsSession)
	}

	return Clients[clientKey]
}

/**
 * Capture the incoming request from the Router Node that needs to be
 * forwarded to the Data Runtime. Use the access key sever sercret access key
 * to sign the request that is forwarded to the data runtime. When the request
 * is an internal request used for administration, sign the request differently.
 */
func prepareRequest(request *RuntimeRequestObject, internal bool) *RuntimeRequest {
	queryString := ""

	if len(strings.Split(request.Path, "?")) > 1 {
		queryString = strings.Split(request.Path, "?")[1]
	}

	server := map[string]interface{}{
		"REQUEST_METHOD":     request.Method,
		"REQUEST_URI":        request.Path,
		"QUERY_STRING":       queryString,
		"HTTP_CONTENT_TYPE":  request.Headers["Content-Type"],
		"HTTP_HOST":          request.Headers["Host"],
		"HTTP_AUTHORIZATION": request.Headers["Authorization"],
		"HTTP_X_LBDB_DATE":   request.Headers["X-Lbdb-Date"],
	}

	connectionKey, err := auth.SecretsManager().GetConnectionKey(request.DatabaseUuid, request.BranchUuid)

	if err != nil {
		log.Fatal(err)

		return nil
	}

	if internal {
		accessKeyId := string(sha1.New().Sum([]byte(config.Get("host"))))
		accessKeySecret := string(sha256.New().Sum(
			[]byte(strings.Join([]string{connectionKey, config.Get("region"), config.Get("host"), config.Get("env")}, ":")),
		))

		server["HTTP_AUTHORIZATION"] = auth.SignRequest(
			accessKeyId,
			accessKeySecret,
			request.Method,
			request.Path,
			request.Headers,
			request.Body,
			request.Query,
		)
	} else {
		serverAccessKeySecret, err := auth.SecretsManager().GetServerSecret(request.DatabaseUuid, request.AccessKeyId)

		if err != nil {
			log.Fatal(err)

			return nil
		}

		server["HTTP_SERVER_AUTHORIZATION"] = auth.SignRequest(
			request.AccessKeyId,
			serverAccessKeySecret,
			request.Method,
			request.Path,
			request.Headers,
			request.Body,
			request.Query,
		)
	}

	return &RuntimeRequest{
		BranchUuid:   request.BranchUuid,
		DatabaseUuid: request.DatabaseUuid,
		Body:         request.Body,
		Method:       request.Method,
		Path:         request.Path,
		Server:       server,
	}
}

func PrepareResponse(startTime time.Time, executionContext int, res []byte) *RuntimeResponse {
	if res == nil {
		return &RuntimeResponse{
			StatusCode: 500,
			Body: map[string]interface{}{
				"Message": "Internal server error",
				"Status":  "error",
			},
		}
	}

	response := &RuntimeResponse{}
	err := json.Unmarshal([]byte(res), response)

	if err != nil {
		return &RuntimeResponse{
			StatusCode: 500,
			Body: map[string]interface{}{
				"message": "Internal server error",
				"status":  "error",
			},
		}
	}

	response.Body["_execution_context"] = executionContext
	response.Body["_execution_latency"] = float64(time.Since(startTime)) / float64(time.Millisecond)

	return response

}
