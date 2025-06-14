package storage

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	p "path"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
	"github.com/litebase/litebase/common/config"
	internalStorage "github.com/litebase/litebase/internal/storage"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
)

type ObjectFileSystemDriver struct {
	bucket   string
	buffers  sync.Pool
	context  context.Context
	S3Client *s3.Client
}

func NewObjectFileSystemDriver(c *config.Config) *ObjectFileSystemDriver {
	ctx := context.Background()

	sdkConfig, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(c.StorageRegion),
		awsConfig.WithBaseEndpoint(c.StorageEndpoint),
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.StorageAccessKeyId,
				c.StorageSecretAccessKey,
				"",
			),
		),
		func(o *awsConfig.LoadOptions) error {
			if !c.FakeObjectStorage {
				return nil
			}

			o.HTTPClient = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					// Override the dial address because the SDK uses the bucket name as a subdomain.
					DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
						dialer := net.Dialer{
							Timeout:   30 * time.Second,
							KeepAlive: 30 * time.Second,
						}

						var s3ServerUrl *url.URL

						if s3Server != nil {
							s3ServerUrl, _ = url.Parse(s3Server.URL)
						} else {
							s3ServerUrl, _ = url.Parse(c.StorageEndpoint)
						}

						return dialer.DialContext(ctx, network, s3ServerUrl.Host)
					},
				},
			}

			return nil
		},
	)

	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return nil
	}

	s3Client := s3.NewFromConfig(sdkConfig, func(o *s3.Options) {
		if c.FakeObjectStorage {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String(c.StorageEndpoint)
		}
	})

	driver := &ObjectFileSystemDriver{
		bucket: c.StorageBucket,
		buffers: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
		context:  context.Background(),
		S3Client: s3Client,
	}

	return driver
}

func (fs *ObjectFileSystemDriver) ClearFiles() error {
	entries, err := fs.ReadDir("")

	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			err = fs.RemoveAll(entry.Name())

			if err != nil {
				return err
			}
		} else {
			err = fs.Remove(entry.Name())

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (fs *ObjectFileSystemDriver) Create(path string) (internalStorage.File, error) {
	_, err := fs.S3Client.PutObject(fs.context, &s3.PutObjectInput{
		Bucket: &fs.bucket,
		Key:    &path,
		Body:   bytes.NewReader([]byte{}),
	})

	if err != nil {
		log.Println("Error creating file", err)
		return nil, err
	}

	return NewObjectFile(fs, path, os.O_CREATE, true)
}

func (fs *ObjectFileSystemDriver) EnsureBucketExists() {
	// Check if the bucket exists
	_, err := fs.S3Client.HeadBucket(fs.context, &s3.HeadBucketInput{
		Bucket: aws.String(fs.bucket),
	})

	if err == nil {
		log.Println("Bucket already exists:", fs.bucket)
		return
	}

	_, err = fs.S3Client.CreateBucket(fs.context, &s3.CreateBucketInput{
		Bucket: aws.String(fs.bucket),
	})

	if err != nil {
		log.Fatalf("failed to create bucket, %v", err)
	}
}

func (fs *ObjectFileSystemDriver) Flush() error {
	// No-op
	return nil
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
	return NewObjectFile(fs, path, os.O_RDWR, false)
}

func (fs *ObjectFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	return NewObjectFile(fs, path, flag, false)
}

func (fs *ObjectFileSystemDriver) OpenFileDirect(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	return fs.OpenFile(path, flag, perm)
}

func (fs *ObjectFileSystemDriver) Path(path string) string {
	return path
}

// Read the directory using S3
func (fs *ObjectFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		MaxKeys: aws.Int32(1000),
		Prefix:  aws.String(path),
	}

	// if path != "/" {
	// 	input.Delimiter = aws.String("/")
	// }

	paginator := s3.NewListObjectsV2Paginator(fs.S3Client, input)

	entries := make([]internalStorage.DirEntry, 0)

	for paginator.HasMorePages() {
		response, err := paginator.NextPage(fs.context)

		if err != nil {
			var noKey *s3types.NoSuchKey
			var notFound *s3types.NotFound

			if errors.As(err, &notFound) || errors.As(err, &noKey) {
				return nil, os.ErrNotExist
			}

			return nil, err
		}

		for _, obj := range response.Contents {
			key := p.Base(*obj.Key)

			entries = append(entries,
				internalStorage.NewDirEntry(
					key,
					false,
					NewStaticFileInfo(key, *obj.Size, *obj.LastModified),
				),
			)
		}

		for _, prefix := range response.CommonPrefixes {
			key := p.Base(*prefix.Prefix)

			entries = append(entries,
				internalStorage.NewDirEntry(
					strings.TrimRight(key, "/"),
					true,
					NewStaticFileInfo(key, 0, time.Time{}),
				),
			)
		}
	}

	return entries, nil
}

func (fs *ObjectFileSystemDriver) ReadFile(path string) ([]byte, error) {
	// Read the file using S3
	output, err := fs.S3Client.GetObject(fs.context, &s3.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		var noKey *s3types.NoSuchKey
		var notFound *s3types.NotFound

		if errors.As(err, &notFound) || errors.As(err, &noKey) {
			return nil, os.ErrNotExist
		}

		log.Println("Error reading file", err)

		return nil, err
	}

	if *output.ContentLength == 0 {
		return nil, nil
	}

	body, err := io.ReadAll(output.Body)

	if err != nil {
		log.Println("Error reading file body", err)
		return nil, err
	}

	defer output.Body.Close()

	decompressed, err := s2.Decode(nil, body)

	if err != nil {
		log.Println("Error decompressing file", err, len(body))
		return nil, err
	}

	return decompressed, nil
}

func (fs *ObjectFileSystemDriver) Remove(path string) error {
	_, err := fs.S3Client.DeleteObject(fs.context, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		return err
	}

	return nil
}

func (fs *ObjectFileSystemDriver) RemoveAll(path string) error {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		// Delimiter: aws.String("/"),
		MaxKeys: aws.Int32(1000),
		Prefix:  aws.String(path),
	}

	paginator := s3.NewListObjectsV2Paginator(fs.S3Client, input)

	for paginator.HasMorePages() {
		response, err := paginator.NextPage(fs.context)

		if err != nil {
			return err
		}

		objectsToDelete := make([]s3types.ObjectIdentifier, len(response.Contents))

		for i, object := range response.Contents {
			objectsToDelete[i] = s3types.ObjectIdentifier{Key: object.Key}
		}

		_, err = fs.S3Client.DeleteObjects(fs.context, &s3.DeleteObjectsInput{
			Bucket: aws.String(fs.bucket),
			Delete: &s3types.Delete{
				Objects: objectsToDelete,
			},
		})

		if err != nil {
			return err
		}

		if len(response.Contents) == 0 {
			break
		}
	}

	return nil
}

// Perform a copy operation to do a rename
func (fs *ObjectFileSystemDriver) Rename(oldKey, newKey string) error {
	_, err := fs.S3Client.CopyObject(fs.context, &s3.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", fs.bucket, oldKey)),
		Key:        aws.String(newKey),
	})

	if err != nil {
		log.Println("Error copying object", err)
		return err
	}

	_, err = fs.S3Client.DeleteObject(fs.context, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(oldKey),
	})

	if err != nil {
		return err
	}

	return nil

}

func (fs *ObjectFileSystemDriver) Shutdown() error {
	return nil
}

func (fs *ObjectFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	// If the paths ends with a slash, it's a directory
	if strings.HasSuffix(path, "/") {
		return NewStaticFileInfo(path, 0, time.Now().UTC()), nil
	}

	output, err := fs.S3Client.HeadObject(fs.context, &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		var noKey *s3types.NoSuchKey
		var notFound *s3types.NotFound

		if errors.As(err, &notFound) || errors.As(err, &noKey) {
			return nil, os.ErrNotExist
		}

		return nil, err
	}

	return NewStaticFileInfo(path, *output.ContentLength, *output.LastModified), nil
}

func (fs *ObjectFileSystemDriver) Truncate(name string, size int64) error {
	// This is a no-op since we can't truncate files in S3

	return fmt.Errorf("truncate not implemented for object storage")
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

	_, err := fs.S3Client.PutObject(fs.context, &s3.PutObjectInput{
		Body:        bytes.NewReader(compressionBuffer.Bytes()),
		Bucket:      aws.String(fs.bucket),
		Key:         aws.String(path),
		ContentType: aws.String("application/octet-stream"),
	})

	if err != nil {
		return err
	}

	return nil
}
