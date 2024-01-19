package storage

import (
	"context"
	"io/fs"
	internalStorage "litebasedb/internal/storage"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

type LambdaFileSystemDriver struct {
	client *lambda.Client
}

func newLambdaClient() *lambda.Client {
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

	cfg, err := config.LoadDefaultConfig(context.Background(),
		configFunctions...,
	)

	if err != nil {
		panic(err)
	}

	return lambda.NewFromConfig(cfg)
}

func NewLambdaFileSystemDriver() *LambdaFileSystemDriver {
	return &LambdaFileSystemDriver{
		client: newLambdaClient(),
	}
}

func (lfsd *LambdaFileSystemDriver) Create(path string) (internalStorage.File, error) {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("create", path))

	if err != nil {
		return nil, err
	}

	return &LambdaFile{
		name:   path,
		client: lfsd.client,
	}, nil
}

func (lfsd *LambdaFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("mkdir", path).WithPerm(perm))

	if err != nil {
		return err
	}

	return nil
}

func (lfsd *LambdaFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("mkdirall", path).WithPerm(perm))

	if err != nil {
		return err
	}

	return nil
}

func (lfsd *LambdaFileSystemDriver) Open(name string) (internalStorage.File, error) {
	response, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("open", name))

	if err != nil {
		return nil, err
	}

	return &LambdaFile{
		bytes:      response.Bytes,
		client:     lfsd.client,
		name:       response.Path,
		data:       response.Data,
		totalBytes: response.TotalBytes,
	}, nil
}

func (lfsd *LambdaFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	response, err := remoteFileProcedure(
		lfsd.client,
		NewFilesystemRequest("openfile", path).WithFlag(flag).WithPerm(perm),
	)

	if err != nil {
		return nil, err
	}

	return &LambdaFile{
		bytes:      response.Bytes,
		client:     lfsd.client,
		name:       response.Path,
		data:       response.Data,
		totalBytes: response.TotalBytes,
	}, nil
}

func (lfsd *LambdaFileSystemDriver) ReadDir(path string) ([]os.DirEntry, error) {
	response, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("readdir", path))

	if err != nil {
		return nil, err
	}

	dirEntries := []os.DirEntry{}

	for _, dirEntry := range response.DirEntries {
		entry := &RemoteDirEntry{}

		if dirEntry["fileInfo"] != nil {
			entry.fileInfo = dirEntry["fileInfo"].(map[string]interface{})
		}

		if dirEntry["isDir"] != nil {
			entry.isDir = dirEntry["isDir"].(bool)
		}

		if dirEntry["mode"] != nil {
			entry.mode = int32(dirEntry["mode"].(float64))
		}

		if dirEntry["name"] != nil {
			entry.name = dirEntry["name"].(string)
		}

		dirEntries = append(dirEntries, entry)
	}

	return dirEntries, nil
}

func (lfsd *LambdaFileSystemDriver) ReadFile(path string) ([]byte, error) {
	response, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("readfile", path))

	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

func (lfsd *LambdaFileSystemDriver) Remove(path string) error {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("remove", path))

	if err != nil {
		return err
	}

	return nil
}

func (lfsd *LambdaFileSystemDriver) RemoveAll(path string) error {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("removeall", path))

	if err != nil {
		return err
	}

	return nil
}

func (lfsd *LambdaFileSystemDriver) Rename(oldpath, newpath string) error {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("rename", oldpath).WithNewPath(newpath))

	if err != nil {
		return err
	}

	return nil
}

func (lfsd *LambdaFileSystemDriver) Stat(path string) (fs.FileInfo, error) {
	response, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("stat", path))

	if err != nil {
		return nil, err
	}

	fileInfo := &RemoteFileInfo{
		name: response.Path,
	}

	if response.Stat["mode"] != nil {
		fileInfo.mode = int32(response.Stat["mode"].(float64))
	}

	if response.Stat["size"] != nil {
		fileInfo.size = int64(response.Stat["size"].(float64))
	}

	if response.Stat["modTime"] != nil {
		parsedTime, err := time.Parse(time.RFC3339, response.Stat["modTime"].(string))

		if err != nil {
			return nil, err
		}

		fileInfo.modTime = parsedTime
	}

	if response.Stat["isDir"] != nil {
		fileInfo.isDir = response.Stat["isDir"].(bool)
	}

	return fileInfo, nil
}

func (lfsd *LambdaFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	_, err := remoteFileProcedure(lfsd.client, NewFilesystemRequest("writefile", path).WithData(data).WithPerm(perm))

	if err != nil {
		return err
	}

	return nil
}
