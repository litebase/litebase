package storage

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"io"
	"os"
	"strings"

	internalStorage "litebasedb/internal/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type S3FileSystemDriver struct {
	client          *s3.Client
	contentEncoding string
	checksumSHA1    []byte
}

func newS3Client() *s3.Client {
	configFunctions := []func(*config.LoadOptions) error{}

	if os.Getenv("AWS_S3_ENDPOINT") != "" {
		s3Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: os.Getenv("AWS_S3_ENDPOINT"),
			}, nil
		})

		configFunctions = append(configFunctions, config.WithEndpointResolverWithOptions(s3Resolver))
	}

	configFunctions = append(configFunctions, config.WithRegion("us-east-1"))

	sdkConfig, err := config.LoadDefaultConfig(context.TODO(),
		configFunctions...,
	)

	if err != nil {
		panic(err)
	}

	return s3.NewFromConfig(sdkConfig, func(options *s3.Options) {
		if os.Getenv("APP_ENV") == "local" {
			options.UsePathStyle = true
		}
	})
}

func NewS3FileSystemDriver() *S3FileSystemDriver {
	return &S3FileSystemDriver{
		client: newS3Client(),
	}
}

func (fs *S3FileSystemDriver) Create(path string) (internalStorage.File, error) {
	_, err := fs.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(path)),
	})

	if err != nil {
		return nil, err
	}

	return &S3File{
		name:   path,
		client: fs.client,
	}, nil
}

func (fs *S3FileSystemDriver) Mkdir(path string, perm os.FileMode) error {
	// Not required for S3

	return nil
}

func (fs *S3FileSystemDriver) MkdirAll(path string, perm os.FileMode) error {
	// Not required for S3

	return nil
}

func (fs *S3FileSystemDriver) Open(name string) (internalStorage.File, error) {
openFile:
	response, err := fs.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(name)),
	})

	if err != nil {
		var awsError *awshttp.ResponseError

		if errors.As(err, &awsError) {
			if awsError.HTTPStatusCode() == 404 {
				// Create the file if it doesn't exist
				_, err := fs.client.PutObject(context.TODO(), &s3.PutObjectInput{
					Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
					Key:    aws.String(preparePath(name)),
				})

				if err != nil {
					return nil, err
				}

				goto openFile
			}
		}

		return nil, err
	}

	data := make([]byte, *response.ContentLength)

	response.Body.Read(data)

	return &S3File{
		bytes:      *response.ContentLength,
		client:     fs.client,
		data:       data,
		name:       name,
		totalBytes: *response.ContentLength,
	}, nil
}

func (fs *S3FileSystemDriver) OpenFile(name string, flag int, perm os.FileMode) (internalStorage.File, error) {
openFile:
	response, err := fs.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(name)),
	})

	if err != nil {
		var awsError *awshttp.ResponseError

		if errors.As(err, &awsError) {
			if awsError.HTTPStatusCode() == 404 {
				if flag&os.O_CREATE == 0 {
					return nil, os.ErrNotExist
				}

				// Create the file if it doesn't exist
				_, err := fs.client.PutObject(context.TODO(), &s3.PutObjectInput{
					Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
					Key:    aws.String(preparePath(name)),
				})

				if err != nil {
					return nil, err
				}

				goto openFile
			}
		}
	}

	data := make([]byte, *response.ContentLength)

	response.Body.Read(data)

	return &S3File{
		bytes:      *response.ContentLength,
		client:     fs.client,
		data:       data,
		name:       name,
		totalBytes: *response.ContentLength,
	}, nil
}

func (fs *S3FileSystemDriver) ReadDir(path string) ([]os.DirEntry, error) {
	dirEnties := []os.DirEntry{}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(os.Getenv("AWS_S3_BUCKET")),
		Prefix:  aws.String(preparePath(path)),
		MaxKeys: aws.Int32(1000),
	}

	for {
		output, err := fs.client.ListObjectsV2(context.TODO(), input)

		if err != nil {
			return nil, err
		}

		for _, object := range output.Contents {
			entry := &RemoteDirEntry{}
			entry.name = *object.Key
			entry.isDir = strings.HasSuffix(*object.Key, "/")
			entry.mode = 0666
			entry.fileInfo = map[string]interface{}{
				"isDir":   entry.isDir,
				"modTime": *object.LastModified,
				"mode":    entry.mode,
				"name":    *object.Key,
				"size":    *object.Size,
			}

			dirEnties = append(dirEnties, entry)
		}

		input.ContinuationToken = output.NextContinuationToken

		if output.IsTruncated != nil && !*output.IsTruncated {
			break
		}
	}

	return dirEnties, nil
}

func (fs *S3FileSystemDriver) ReadFile(path string) ([]byte, error) {
	response, err := fs.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket:                  aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:                     aws.String(preparePath(path)),
		ResponseContentEncoding: aws.String(fs.contentEncoding),
	})

	if err != nil {
		var awsError *awshttp.ResponseError

		if errors.As(err, &awsError) {
			if awsError.HTTPStatusCode() == 404 {
				return nil, os.ErrNotExist
			}
		}

		return nil, err
	}

	data, err := io.ReadAll(response.Body)

	if err != nil && err.Error() != "EOF" {
		return nil, err
	}

	return data, nil
}

func (fs *S3FileSystemDriver) Remove(path string) error {
	_, err := fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(path)),
	})

	if err != nil {
		if strings.Contains(err.Error(), "NoSuchKey") {
			return os.ErrNotExist
		}

		return err
	}

	return nil
}

func (fs *S3FileSystemDriver) RemoveAll(path string) error {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String("bucket"),
		MaxKeys: aws.Int32(100),
	}

	for {
		output, err := fs.client.ListObjectsV2(context.Background(), input)
		if err != nil {
			return err
		}

		if len(output.Contents) == 0 {
			break
		}

		objects := make([]types.ObjectIdentifier, len(output.Contents))

		for i, object := range output.Contents {
			objects[i] = types.ObjectIdentifier{
				Key: object.Key,
			}
		}

		_, err = fs.client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String("bucket"),
			Delete: &types.Delete{
				Objects: objects,
			},
		})

		if err != nil {
			return err
		}

		input.ContinuationToken = output.NextContinuationToken

		if output.IsTruncated != nil && !*output.IsTruncated {
			break
		}
	}

	return nil
}

func (fs *S3FileSystemDriver) Rename(oldpath, newpath string) error {
	_, err := fs.client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     aws.String(os.Getenv("AWS_S3_BUCKET")),
		CopySource: aws.String(os.Getenv("AWS_S3_BUCKET") + "/" + preparePath(oldpath)),
		Key:        aws.String(preparePath(newpath)),
	})

	if err != nil {
		return err
	}

	_, err = fs.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(oldpath)),
	})

	if err != nil {
		return err
	}

	return nil
}

func (fs *S3FileSystemDriver) Stat(path string) (os.FileInfo, error) {
	output, err := fs.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(path)),
	})

	if err != nil {
		var awsError *awshttp.ResponseError

		if errors.As(err, &awsError) {
			if awsError.HTTPStatusCode() == 404 {
				return nil, os.ErrNotExist
			}
		}

		return nil, err
	}

	fileInfo := &RemoteFileInfo{
		name: path,
	}

	fileInfo.size = *output.ContentLength
	fileInfo.modTime = *output.LastModified
	fileInfo.isDir = false

	return fileInfo, nil
}

func (fs *S3FileSystemDriver) WriteFile(name string, data []byte, perm os.FileMode) error {
	_, err := fs.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("AWS_S3_BUCKET")),
		Key:    aws.String(preparePath(name)),
		Body:   bytes.NewReader(data),
		// ContentEncoding: aws.String(fs.contentEncoding),
		// ContentMD5:      aws.String(string(fs.checksumSHA1)),
		// ChecksumSHA1:    aws.String("SHA1"),
		// PredefinedChecksum: types.ChecksumSHA1,
	})

	if err != nil {
		return err
	}

	return nil
}

func (fs *S3FileSystemDriver) WithEncoding(encoding string) *S3FileSystemDriver {
	fs.contentEncoding = encoding

	return fs
}

func (fs *S3FileSystemDriver) WithChecksumSHA1(checkSumSHA1 []byte) *S3FileSystemDriver {
	fs.checksumSHA1 = checkSumSHA1

	return fs
}

func preparePath(path string) string {
	//replace the data path
	path = strings.Replace(path, os.Getenv("LITEBASEDB_DATA_PATH"), "", 1)

	return path
}

func getPath(path string) string {
	// md5 hash the path
	pathHash := sha1.Sum([]byte(path))

	// convert the hash to a string
	pathHashString := string(pathHash[:])

	return pathHashString
}
