package file

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

type LambdaClient struct {
	client       *lambda.Client
	databaseName string
}

type page struct {
	Error  string `json:"error"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Data   []byte `json:"data"`
}

type response struct {
	Id    string `json:"id"`
	Size  int64  `json:"size"`
	Pages []page `json:"pages"`
}

// Create a new AWS Lambda client
func NewLambdaClient(databaseName string) *LambdaClient {
	configFunctions := []func(*config.LoadOptions) error{}

	if os.Getenv("AWS_LAMBDA_ENDPOINT") != "" {
		lambdaResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: os.Getenv("AWS_LAMBDA_ENDPOINT"),
			}, nil
		})

		configFunctions = append(configFunctions, config.WithEndpointResolverWithOptions(lambdaResolver))
	}

	configFunctions = append(configFunctions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")))
	configFunctions = append(configFunctions, config.WithRegion("us-east-1"))

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		configFunctions...,
	)

	if err != nil {
		panic(err)
	}

	return &LambdaClient{
		client:       lambda.NewFromConfig(cfg),
		databaseName: databaseName,
	}
}

func (lc *LambdaClient) Invoke(input *lambda.InvokeInput) (*lambda.InvokeOutput, error) {
	return lc.client.Invoke(context.Background(), input)
}

// read
func (lc *LambdaClient) ReadAt(data []byte, offset int64) (n int, err error) {
	payload := map[string]interface{}{
		"action":        "read",
		"database_name": lc.databaseName,
		"pages": []map[string]interface{}{
			{
				"offset": offset,
				"length": len(data),
			},
		},
	}

	jsonPayload, err := json.Marshal(payload)

	if err != nil {
		panic(err)
	}

	var input = &lambda.InvokeInput{
		FunctionName: aws.String(os.Getenv("AWS_LAMBDA_FUNCTION_NAME")),
		Payload:      jsonPayload,
	}

	output, err := lc.client.Invoke(context.Background(), input)

	if err != nil {
		panic(err)
	}

	response := map[string]interface{}{}

	err = json.Unmarshal(output.Payload, &response)

	if err != nil {
		panic(err)
	}

	for i, p := range response["pages"].([]interface{}) {
		page := p.(map[string]interface{})
		pageData := page["data"].(string)
		decodedData, err := base64.StdEncoding.DecodeString(pageData)

		if err != nil {
			panic(err)
		}

		// if pageOffset == offset && pageLength == int64(len(data)) {
		if page["error"].(string) != "" {
			if page["error"].(string) == "EOF" {
				return len(decodedData), io.EOF
			}

			return len(decodedData), errors.New(page["error"].(string))
		}

		n = copy(data, decodedData)
		err = nil

		// Take the first page and break, for now
		if i == 0 {
			break
		}
	}

	return n, nil
}

// write
func (lc *LambdaClient) Write(pages []struct {
	Data   []byte
	Length int64
	Offset int64
}) (*response, error) {
	payload := map[string]interface{}{
		"action":        "write",
		"database_name": lc.databaseName,
		"pages":         pages,
	}

	jsonPayload, err := json.Marshal(payload)

	if err != nil {
		panic(err)
	}

	var input = &lambda.InvokeInput{
		FunctionName: aws.String(os.Getenv("AWS_LAMBDA_FUNCTION_NAME")),
		Payload:      jsonPayload,
	}

	output, err := lc.client.Invoke(context.Background(), input)

	if err != nil {
		panic(err)
	}

	r := &response{}

	err = json.Unmarshal(output.Payload, r)

	// for _, page := range r.Pages {
	// 	// pageOffset := int64(page["offset"].(float64))
	// 	// pageLength := int64(page["length"].(float64))

	// 	if page.Error != "" {
	// 		return 0, errors.New(page.Error)
	// 	}

	// 	return int(page.Length), nil
	// }

	return r, err
}

func (lc *LambdaClient) Size() (int64, error) {
	payload := map[string]interface{}{
		"action":        "size",
		"database_name": lc.databaseName,
	}

	jsonPayload, err := json.Marshal(payload)

	if err != nil {
		panic(err)
	}

	var input = &lambda.InvokeInput{
		FunctionName: aws.String(os.Getenv("AWS_LAMBDA_FUNCTION_NAME")),
		Payload:      jsonPayload,
	}

	output, err := lc.client.Invoke(context.Background(), input)

	if err != nil {
		panic(err)
	}

	response := map[string]interface{}{}

	err = json.Unmarshal(output.Payload, &response)

	if err != nil {
		panic(err)
	}

	// if response["error"].(string) != "" {
	// 	return 0, errors.New(response["error"].(string))
	// }

	return int64(response["size"].(float64)), nil
}
