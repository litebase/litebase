package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

type FilesystemRequest struct {
	Action  string      `json:"action"`
	NewPath string      `json:"newPath"`
	Path    string      `json:"path"`
	Perm    fs.FileMode `json:"perm"`
	Flag    int         `json:"flag"`
	Data    []byte      `json:"data"`
	Offset  int64       `json:"offset"`
}

func NewFilesystemRequest(action string, path string) FilesystemRequest {
	return FilesystemRequest{
		Action: action,
		Path:   path,
	}
}

func (request FilesystemRequest) WithData(data []byte) FilesystemRequest {
	request.Data = data

	return request
}

func (request FilesystemRequest) WithFlag(flag int) FilesystemRequest {
	request.Flag = flag

	return request
}

func (request FilesystemRequest) WithNewPath(newPath string) FilesystemRequest {
	request.NewPath = newPath

	return request
}

func (request FilesystemRequest) WithOffset(offset int64) FilesystemRequest {
	request.Offset = offset

	return request
}

func (request FilesystemRequest) WithPath(path string) FilesystemRequest {
	request.Path = path

	return request
}

func (request FilesystemRequest) WithPerm(perm fs.FileMode) FilesystemRequest {
	request.Perm = perm

	return request
}

func remoteFileProcedure(client *lambda.Client, filesystemRequest FilesystemRequest) (FilesystemResponse, error) {
	jsonPayload, err := json.Marshal(map[string]interface{}{
		"type":              "fs",
		"filesystemRequest": filesystemRequest,
	})

	if err != nil {
		panic(err)
	}

	var input = &lambda.InvokeInput{
		FunctionName: aws.String(os.Getenv("AWS_LAMBDA_FUNCTION_NAME")),
		Payload:      jsonPayload,
	}

	output, err := client.Invoke(context.Background(), input)

	if err != nil {
		return FilesystemResponse{}, err
	}

	response := &Response{}

	err = json.Unmarshal(output.Payload, response)

	if err != nil {
		return FilesystemResponse{}, err
	}

	if response.FilesystemResponse.Error != "" {
		if strings.Contains(response.FilesystemResponse.Error, "no such file or directory") {
			return FilesystemResponse{}, os.ErrNotExist
		}

		return FilesystemResponse{}, errors.New(response.FilesystemResponse.Error)
	}

	return response.FilesystemResponse, nil
}
