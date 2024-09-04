package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"io/fs"
	"litebase/internal/config"
	"log"
	"os"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/s2"
)

type ObjectFile struct {
	client         *s3.Client
	data           []byte
	fileInfo       *ObjectFileInfo
	Key            string
	openFlags      int
	sha256Checksum [32]byte
}

func NewObjectFile(client *s3.Client, key string, openFlags int) *ObjectFile {
	return &ObjectFile{
		client: client,
		data:   []byte{},
		fileInfo: &ObjectFileInfo{
			name:    key,
			size:    0,
			modTime: time.Now(),
		},
		Key:            key,
		openFlags:      openFlags,
		sha256Checksum: sha256.Sum256([]byte{}),
	}
}

// If changes have been made to the file, this will upload the changes to the
// object store upon closing the file.
func (o *ObjectFile) Close() error {
	if len(o.data) == 0 {
		return nil
	}

	if o.sha256Checksum == sha256.Sum256(o.data) {
		return nil
	}

	// Fail silently if the file is read-only
	if o.openFlags == os.O_RDONLY {
		return nil
	}

	compressed := s2.Encode(nil, o.data)

	_, err := o.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(config.Get().StorageBucket),
		Key:    aws.String(o.Key),
		Body:   bytes.NewReader(compressed),
	})

	if err != nil {
		log.Println("Error closing file", err)
		return err
	}

	return nil
}

// Read bytes from the file.
func (o *ObjectFile) Read(p []byte) (n int, err error) {
	if len(o.data) == 0 {
		return 0, io.EOF
	}

	n = copy(p, o.data)

	if n == len(o.data) {
		err = io.EOF
	}

	return n, err
}

// Read bytes from the file at a specific offset.
func (o *ObjectFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(o.data) == 0 {
		return 0, io.EOF
	}

	if off > int64(len(o.data)) {
		return 0, io.EOF
	}

	n = copy(p, o.data[off:])

	return n, nil
}

func (o *ObjectFile) Seek(offset int64, whence int) (int64, error) {
	if len(o.data) == 0 {
		return 0, io.EOF
	}

	switch whence {
	case io.SeekStart:
		if offset > int64(len(o.data)) {
			return 0, io.EOF
		}

		return offset, nil
	case io.SeekCurrent:
		if offset+int64(len(o.data)) > int64(len(o.data)) {
			return 0, io.EOF
		}

		return offset + int64(len(o.data)), nil
	case io.SeekEnd:
		if offset+int64(len(o.data)) > int64(len(o.data)) {
			return 0, io.EOF
		}

		return offset + int64(len(o.data)), nil
	}

	return 0, nil
}

// Return stats about the file.
func (o *ObjectFile) Stat() (fs.FileInfo, error) {
	return o.fileInfo, nil
}

// Sync the file with the object store.
func (o *ObjectFile) Sync() error {
	if o.openFlags == os.O_RDONLY {
		return os.ErrPermission
	}

	compressed := s2.Encode(nil, o.data)

	_, err := o.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(config.Get().StorageBucket),
		Key:    aws.String(o.Key),
		Body:   bytes.NewReader(compressed),
	})

	if err != nil {
		log.Println("Error syncing file", err)
		return err
	}

	return nil
}

// Resize the file to a specific size.
func (o *ObjectFile) Truncate(size int64) error {
	if o.openFlags == os.O_RDONLY {
		return os.ErrPermission
	}

	if size == 0 {
		o.data = []byte{}
	}

	if size > int64(len(o.data)) {
		o.data = slices.Grow(o.data, int(size))
	}

	if size < int64(len(o.data)) {
		o.data = o.data[:size]
	}

	compressed := s2.Encode(nil, o.data)

	_, err := o.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(config.Get().StorageBucket),
		Key:    aws.String(o.Key),
		Body:   bytes.NewReader(compressed),
	})

	if err != nil {
		log.Println("Error truncating file", err)
		return err
	}

	return nil
}

func (o *ObjectFile) WithData(data []byte) *ObjectFile {
	if len(data) > 0 {
		o.data = append(o.data, data...)
	}

	return o
}

// Write bytes to the file at the current offset.
func (o *ObjectFile) Write(p []byte) (n int, err error) {
	if o.openFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	o.data = append(o.data, p...)

	return len(p), nil
}

func (o *ObjectFile) WriteAt(p []byte, off int64) (n int, err error) {
	if o.openFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	if off > int64(len(o.data)) {
		return 0, io.EOF
	}

	o.data = append(o.data[:off], append(p, o.data[off:]...)...)

	return len(p), nil
}

func (o *ObjectFile) WriteTo(w io.Writer) (n int64, err error) {
	if o.openFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	bytesWritten, err := w.Write(o.data)

	if err != nil {
		return 0, err
	}

	return int64(bytesWritten), nil
}

func (o *ObjectFile) WriteString(s string) (ret int, err error) {
	if o.openFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	o.data = append(o.data, []byte(s)...)

	return len(s), nil
}
