package storage

import (
	"bytes"
	"fmt"
	"io/fs"
	"log"
	"os"
	p "path"
	"strings"
	"sync"
	"time"

	"litebase/internal/config"
	internalStorage "litebase/internal/storage"

	"github.com/klauspost/compress/s2"
)

type ObjectFileSystemDriver struct {
	bucket   string
	buffers  sync.Pool
	S3Client *S3Client
}

func NewObjectFileSystemDriver(c *config.Config) *ObjectFileSystemDriver {
	driver := &ObjectFileSystemDriver{
		bucket: c.StorageBucket,
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
		S3Client: NewS3Client(
			c,
			c.StorageBucket,
			c.StorageRegion,
		),
	}

	return driver
}

func (fs *ObjectFileSystemDriver) Create(path string) (internalStorage.File, error) {
	_, err := fs.S3Client.PutObject(path, []byte{})

	if err != nil {
		log.Println("Error creating file", err)
		return nil, err
	}

	return NewObjectFile(fs.S3Client, path, os.O_CREATE), nil
}

func (fs *ObjectFileSystemDriver) EnsureBucketExists() {
	// Check if the bucket exists
	headBucketOutput, _ := fs.S3Client.HeadBucket()

	if headBucketOutput != (HeadBucketResponse{}) {
		return
	}

	_, err := fs.S3Client.CreateBucket()

	if err != nil {
		log.Printf("failed to create bucket, %v", err)
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
	return NewObjectFile(fs.S3Client, path, os.O_RDWR), nil
}

func (fs *ObjectFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	// TODO: Handle the create flag
	// TODO: Read the data from object storage and place in the file data
	return NewObjectFile(fs.S3Client, path, flag), nil
}

// Read the directory using S3
func (fs *ObjectFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	input := ListObjectsV2Input{
		MaxKeys: 1000,
		Prefix:  path,
	}

	if path != "" {
		input.Delimiter = "/"
	}

	paginator := NewListObjectsV2Paginator(fs.S3Client, input)

	entries := make([]internalStorage.DirEntry, 0)

	for paginator.HasMorePages() {
		response, err := paginator.NextPage()

		if err != nil {
			if response.StatusCode == 404 {
				return nil, os.ErrNotExist
			}

			return nil, err
		}

		for _, obj := range response.ListBucketResult.Contents {
			key := p.Base(obj.Key)

			entries = append(entries,
				internalStorage.NewDirEntry(
					key,
					false,
					NewStaticFileInfo(key, obj.Size, obj.LastModified.Time),
				),
			)
		}

		for _, prefix := range response.ListBucketResult.CommonPrefixes {
			key := p.Base(prefix)

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
	resp, err := fs.S3Client.GetObject(path)

	if err != nil {
		if resp.StatusCode == 404 {
			return nil, os.ErrNotExist
		}

		log.Println("Error reading file", err)

		return nil, err
	}

	if len(resp.Data) == 0 {
		return nil, nil
	}

	decompressed, err := s2.Decode(nil, resp.Data)

	if err != nil {
		log.Println("Error decompressing file", err, len(resp.Data))
		return nil, err
	}

	return decompressed, nil
}

func (fs *ObjectFileSystemDriver) Remove(path string) error {
	err := fs.S3Client.DeleteObject(path)

	if err != nil {
		return err
	}

	return nil
}

func (fs *ObjectFileSystemDriver) RemoveAll(path string) error {
	input := ListObjectsV2Input{
		Delimiter: "/",
		MaxKeys:   1000,
		Prefix:    path,
	}

	paginator := NewListObjectsV2Paginator(fs.S3Client, input)

	for paginator.HasMorePages() {
		response, err := paginator.NextPage()

		if err != nil {
			return err
		}

		objectsToDelete := make([]string, len(response.ListBucketResult.Contents))

		for i, object := range response.ListBucketResult.Contents {
			objectsToDelete[i] = object.Key
		}

		err = fs.S3Client.DeleteObjects(objectsToDelete)

		if err != nil {
			return err
		}

		if len(response.ListBucketResult.Contents) == 0 {
			break
		}
	}

	return nil
}

// Perform a copy operation to do a rename
func (fs *ObjectFileSystemDriver) Rename(oldKey, newKey string) error {
	err := fs.S3Client.CopyObject(oldKey, newKey)

	if err != nil {
		return err
	}

	err = fs.S3Client.DeleteObject(oldKey)

	if err != nil {
		return err
	}

	return nil

}

func (fs *ObjectFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	// If the paths ends with a slash, it's a directory
	if strings.HasSuffix(path, "/") {
		return NewStaticFileInfo(path, 0, time.Now()), nil
	}

	result, err := fs.S3Client.HeadObject(path)

	if err != nil {
		if result.StatusCode == 404 {
			return nil, os.ErrNotExist
		}

		return nil, err
	}

	return NewStaticFileInfo(path, result.ContentLength, result.LastModified), nil
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

	_, err := fs.S3Client.PutObject(path, compressed)

	if err != nil {
		return err
	}

	return nil
}
