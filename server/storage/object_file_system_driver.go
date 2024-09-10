package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	p "path"
	"strings"
	"sync"

	"litebase/internal/config"
	internalStorage "litebase/internal/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/transport/http"
	"github.com/klauspost/compress/s2"
)

// TODO: Remove AWS SDK

type ObjectFileSystemDriver struct {
	bucket   *string
	buffers  sync.Pool
	client   *s3.Client
	s3Client *S3Client
}

func NewObjectFileSystemDriver() *ObjectFileSystemDriver {
	driver := &ObjectFileSystemDriver{
		bucket: aws.String(config.Get().StorageBucket),
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
		s3Client: NewS3Client(config.Get().StorageBucket, config.Get().StorageRegion),
	}

	// Setup a new config
	cfg, _ := awsConfig.LoadDefaultConfig(
		context.TODO(),
		// awsConfig.WithClientLogMode(aws.LogSigning|aws.LogRequest),
		awsConfig.WithRegion("us-east-1"),
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				os.Getenv("LITEBASE_STORAGE_ACCESS_KEY_ID"),
				os.Getenv("LITEBASE_STORAGE_SECRET_ACCESS_KEY"),
				"",
			),
		),
	)

	driver.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		if config.Get().StorageEndpoint != "" {
			o.BaseEndpoint = aws.String(config.Get().StorageEndpoint)
		}

		if config.Get().StorageMode == "object" && (config.Get().Env == "local" || config.Get().Env == "test") {
			o.UsePathStyle = true
		}
	})

	return driver
}

func (fs *ObjectFileSystemDriver) Create(path string) (internalStorage.File, error) {
	_, err := fs.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: fs.bucket,
		Key:    aws.String(path),
		Body:   bytes.NewReader([]byte{}),
	})

	if err != nil {
		log.Println("Error creating file", err)
		return nil, err
	}

	return NewObjectFile(fs.client, path, os.O_CREATE), nil
}

func (fs *ObjectFileSystemDriver) EnsureBucketExists() {
	// Check if the bucket exists
	headBucketOutput, _ := fs.client.HeadBucket(context.TODO(), &s3.HeadBucketInput{
		Bucket: fs.bucket,
	})

	if headBucketOutput != nil {
		return
	}

	_, err := fs.client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: fs.bucket,
	})

	if err != nil {
		log.Fatalf("failed to create bucket, %v", err)
	}
}

func (fs *ObjectFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	// This is a no-op since we can't create directories in S3
	return nil
}

func (fs *ObjectFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	// This is a no-op since we can't create directories in S3

	return nil
}

func (fs *ObjectFileSystemDriver) Open(path string) (internalStorage.File, error) {
	return NewObjectFile(fs.client, path, os.O_RDWR), nil
}

func (fs *ObjectFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	// TODO: Handle the create flag
	// TODO: Read the data from object storage and place in the file data
	return NewObjectFile(fs.client, path, flag), nil
}

// Read the directory using S3
func (fs *ObjectFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	paginator := s3.NewListObjectsV2Paginator(fs.client, &s3.ListObjectsV2Input{
		Bucket:    fs.bucket,
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(1000),
		Prefix:    aws.String(path),
	})

	entries := make([]internalStorage.DirEntry, 0)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())

		if err != nil {
			var httpError *http.ResponseError

			if ok := errors.As(err, &httpError); ok {
				if httpError.Response.StatusCode == 404 {
					log.Println("Directory does not exist", path)
					break
					// return nil, os.ErrNotExist
				}
			}

			log.Println("Error reading directory", err)
			return nil, err
		}

		for _, obj := range page.Contents {
			key := p.Base(*obj.Key)

			entries = append(entries, internalStorage.DirEntry{
				Name:  key,
				IsDir: false,
			})
		}

		for _, prefix := range page.CommonPrefixes {
			key := p.Base(*prefix.Prefix)

			entries = append(entries, internalStorage.DirEntry{
				Name:  strings.TrimRight(key, "/"),
				IsDir: true,
			})
		}
	}

	return entries, nil
}

func (fs *ObjectFileSystemDriver) ReadFile(path string) ([]byte, error) {
	// Read the file using S3
	result, err := fs.client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: fs.bucket,
		Key:    aws.String(path),
	})

	if err != nil {
		var httpError *http.ResponseError

		if ok := errors.As(err, &httpError); ok {
			if httpError.Response.StatusCode == 404 {
				return nil, os.ErrNotExist
			}
		}

		log.Println("Error reading file", err)

		return nil, err
	}

	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)

	if err != nil {
		log.Println("Error reading file", err)
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	decompressed, err := s2.Decode(nil, data)

	if err != nil {
		log.Println("Error decompressing file", err, len(data))
		return nil, err
	}

	return decompressed, nil
}

func (fs *ObjectFileSystemDriver) Remove(path string) error {
	_, err := fs.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: fs.bucket,
		Key:    aws.String(path),
	})

	if err != nil {
		return err
	}

	return nil
}

func (fs *ObjectFileSystemDriver) RemoveAll(path string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: fs.bucket,
	}

	paginator := s3.NewListObjectsV2Paginator(fs.client, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())

		if err != nil {
			return err
		}

		objectsToDelete := make([]types.ObjectIdentifier, len(page.Contents))

		for i, object := range page.Contents {
			objectsToDelete[i] = types.ObjectIdentifier{
				Key: object.Key,
			}
		}

		for _, obj := range page.Contents {
			_, err := fs.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
				Bucket: fs.bucket,
				Key:    obj.Key,
			})

			if err != nil {
				return err
			}

			deleteInput := &s3.DeleteObjectsInput{
				Bucket: fs.bucket,
				Delete: &types.Delete{
					Objects: objectsToDelete,
				},
			}

			_, err = fs.client.DeleteObjects(context.TODO(), deleteInput)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Perform a copy operation to do a rename
func (fs *ObjectFileSystemDriver) Rename(oldpath, newpath string) error {
	_, err := fs.client.CopyObject(context.TODO(), &s3.CopyObjectInput{
		Bucket:     fs.bucket,
		CopySource: aws.String(fmt.Sprintf("%s/%s", *fs.bucket, oldpath)),
		Key:        aws.String(newpath),
	})

	if err != nil {
		return err
	}

	_, err = fs.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: fs.bucket,
		Key:    aws.String(oldpath),
	})

	if err != nil {
		return err
	}

	return nil

}

func (fs *ObjectFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	// If the paths ends with a slash, it's a directory
	if strings.HasSuffix(path, "/") {
		return &ObjectFileInfo{
			name: path,
			size: 0,
		}, nil
	}

	result, err := fs.client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: fs.bucket,
		Key:    aws.String(path),
	})

	if err != nil {
		var nsk *http.ResponseError

		if ok := errors.As(err, &nsk); ok {
			if nsk.Response.StatusCode == 404 {
				return nil, os.ErrNotExist
			}
		}

		log.Println("Error getting file info", err, path)
		return nil, err
	}

	return &ObjectFileInfo{
		name:    path,
		size:    int64(*result.ContentLength),
		modTime: *result.LastModified,
	}, nil
}

func (fs *ObjectFileSystemDriver) Truncate(name string, size int64) error {
	// This is a no-op since we can't truncate files in S3
	log.Fatalln("Truncate not implemented for object storage")

	return nil
}

func (fs *ObjectFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	compressionBuffer := fs.buffers.Get().(*bytes.Buffer)
	defer fs.buffers.Put(compressionBuffer)

	// Reset the buffer to reuse it
	compressionBuffer.Reset()

	// Ensure the buffer has enough capacity
	compressionBufferCap := compressionBuffer.Cap()
	maxEncodedLen := s2.MaxEncodedLen(len(data))

	if compressionBufferCap < maxEncodedLen {
		compressionBuffer.Grow(maxEncodedLen - compressionBufferCap + 1)
	}

	// Encode the data into the buffer
	compressed := s2.Encode(compressionBuffer.Bytes(), data)

	// Write the encoded data to the buffer
	compressionBuffer.Write(compressed)

	err := fs.s3Client.PutObject(path, compressed)

	if err != nil {
		log.Println("Error writing file", err)
		return err
	}

	return nil
}
