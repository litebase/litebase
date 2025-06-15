package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/klauspost/compress/s2"
)

type ObjectFile struct {
	Data           []byte
	FileInfo       StaticFileInfo
	fs             *ObjectFileSystemDriver
	Key            string
	OpenFlags      int
	readPos        int
	Sha256Checksum [32]byte
}

func NewObjectFile(fs *ObjectFileSystemDriver, key string, openFlags int, preExists bool) (*ObjectFile, error) {
	file := &ObjectFile{
		Data: nil,
		FileInfo: StaticFileInfo{
			StaticName:    key,
			StaticSize:    0,
			StaticModTime: time.Now().UTC(),
		},
		fs:             fs,
		Key:            key,
		OpenFlags:      openFlags,
		Sha256Checksum: sha256.Sum256([]byte{}),
	}

	fileExists := false

	if (openFlags&os.O_CREATE != 0) && preExists {
		fileExists = true
	} else if openFlags&os.O_CREATE != 0 {
		output, err := fs.S3Client.HeadObject(file.fs.context, &s3.HeadObjectInput{
			Bucket: aws.String(file.fs.bucket),
			Key:    aws.String(key),
		})

		if err != nil {
			var noKey *s3types.NoSuchKey
			var notFound *s3types.NotFound

			if !errors.As(err, &notFound) && !errors.As(err, &noKey) {
				log.Println("Error checking file existence", err)
				return nil, err
			}
		}

		if output != nil {
			fileExists = true
		}

		if !fileExists {
			// INVESTIGATE: Do we really need to create an empty file here?
			_, err := file.fs.S3Client.PutObject(file.fs.context, &s3.PutObjectInput{
				Bucket: aws.String(file.fs.bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader([]byte{}),
			})

			if err != nil {
				log.Println("Error creating file", err)
				return nil, err
			}

			file.Data = []byte{}
		}
	}

	if file.Data == nil && (openFlags&os.O_RDONLY != 0 || openFlags&os.O_RDWR != 0) {
		output, err := file.fs.S3Client.GetObject(file.fs.context, &s3.GetObjectInput{
			Bucket: aws.String(file.fs.bucket),
			Key:    aws.String(key),
		})

		if err != nil {
			var noKey *s3types.NoSuchKey
			var notFound *s3types.NotFound

			if errors.As(err, &notFound) || errors.As(err, &noKey) {
				return nil, os.ErrNotExist
			}

			return nil, err
		}

		if *output.ContentLength != 0 {
			body, err := io.ReadAll(output.Body)

			if err != nil {
				log.Println("Error reading file body", err)
				return nil, err
			}

			defer output.Body.Close()

			file.Data, err = s2.Decode(nil, body)

			if err != nil {
				log.Println("Error decoding object", err)
				return nil, err
			}

			file.Sha256Checksum = sha256.Sum256(file.Data)
			file.FileInfo.StaticSize = int64(len(file.Data))
		}
	}

	return file, nil
}

// If changes have been made to the file, this will upload the changes to the
// object store upon closing the file.
func (file *ObjectFile) Close() error {
	if len(file.Data) == 0 {
		return nil
	}

	if file.Sha256Checksum == sha256.Sum256(file.Data) {
		return nil
	}

	// Fail silently if the file is read-only
	if file.OpenFlags == os.O_RDONLY {
		return nil
	}

	compressed := s2.Encode(nil, file.Data)

	_, err := file.fs.S3Client.PutObject(file.fs.context, &s3.PutObjectInput{
		Bucket: aws.String(file.fs.bucket),
		Key:    aws.String(file.Key),
		Body:   bytes.NewReader(compressed),
	})

	if err != nil {
		log.Println("Error closing file", err)
		return err
	}

	return nil
}

// Read bytes from the file.
func (file *ObjectFile) Read(p []byte) (n int, err error) {
	if file.Data == nil {
		output, err := file.fs.S3Client.GetObject(file.fs.context, &s3.GetObjectInput{
			Bucket: aws.String(file.fs.bucket),
			Key:    aws.String(file.Key),
		})

		if err != nil {
			return 0, err
		}

		if *output.ContentLength == 0 {
			return 0, io.EOF
		}

		body, err := io.ReadAll(output.Body)

		if err != nil {
			log.Println("Error reading file body", err)
			return 0, err
		}

		defer output.Body.Close()

		file.Data, err = s2.Decode(nil, body)

		if err != nil {
			log.Println("Error decoding file", err)
			return 0, err
		}

		// Reset read position after fetching new data
		file.readPos = 0
	}

	n = copy(p, file.Data[file.readPos:])

	file.readPos += n

	if file.readPos >= len(file.Data) {
		err = io.EOF
	}

	return n, err
}

// Read bytes from the file at a specific offset.
func (file *ObjectFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(file.Data) == 0 {
		return 0, io.EOF
	}

	if off > int64(len(file.Data)) {
		return 0, io.EOF
	}

	n = copy(p, file.Data[off:])

	return n, nil
}

func (file *ObjectFile) Seek(offset int64, whence int) (int64, error) {
	if len(file.Data) == 0 {
		return 0, io.EOF
	}

	switch whence {
	case io.SeekStart:
		if offset < 0 || offset > int64(len(file.Data)) {
			return 0, io.EOF
		}

		file.readPos = int(offset)

		return offset, nil
	case io.SeekCurrent:
		newPos := int64(file.readPos) + offset

		if newPos < 0 || newPos > int64(len(file.Data)) {
			return 0, io.EOF
		}

		file.readPos = int(newPos)

		return newPos, nil
	case io.SeekEnd:
		newPos := int64(len(file.Data)) + offset

		if newPos < 0 || newPos > int64(len(file.Data)) {
			return 0, io.EOF
		}

		file.readPos = int(newPos)

		return newPos, nil
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
}

// Return stats about the file.
func (file *ObjectFile) Stat() (fs.FileInfo, error) {
	return file.FileInfo, nil
}

// Sync the file with the object store.
func (file *ObjectFile) Sync() error {
	if file.OpenFlags == os.O_RDONLY {
		return os.ErrPermission
	}

	compressed := s2.Encode(nil, file.Data)

	_, err := file.fs.S3Client.PutObject(file.fs.context, &s3.PutObjectInput{
		Body:   bytes.NewReader(compressed),
		Bucket: aws.String(file.fs.bucket),
		Key:    aws.String(file.Key),
	})

	if err != nil {
		log.Println("Error syncing file", err)
		return err
	}

	return nil
}

// Resize the file to a specific size.
func (o *ObjectFile) Truncate(size int64) error {
	if o.OpenFlags == os.O_RDONLY {
		return os.ErrPermission
	}

	if size == 0 {
		o.Data = []byte{}
	}

	if size > int64(len(o.Data)) {
		o.Data = slices.Grow(o.Data, int(size))
	}

	if size < int64(len(o.Data)) {
		o.Data = o.Data[:size]
	}

	compressed := s2.Encode(nil, o.Data)

	_, err := o.fs.S3Client.PutObject(o.fs.context, &s3.PutObjectInput{
		Bucket: aws.String(o.fs.bucket),
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
		o.Data = append(o.Data, data...)
	}

	return o
}

// Write bytes to the file at the current offset.
func (o *ObjectFile) Write(p []byte) (n int, err error) {
	if o.OpenFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	o.Data = append(o.Data[:o.readPos], p...)

	o.readPos += len(p)

	return len(p), nil
}

func (o *ObjectFile) WriteAt(p []byte, off int64) (n int, err error) {
	if o.OpenFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	if off > int64(len(o.Data)) {
		return 0, io.EOF
	}

	o.Data = append(o.Data[:off], p...)

	return len(p), nil
}

func (o *ObjectFile) WriteTo(w io.Writer) (n int64, err error) {
	if o.OpenFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	bytesWritten, err := w.Write(o.Data)

	if err != nil {
		return 0, err
	}

	o.readPos += bytesWritten

	return int64(bytesWritten), nil
}

func (o *ObjectFile) WriteString(s string) (ret int, err error) {
	if o.OpenFlags == os.O_RDONLY {
		return 0, os.ErrPermission
	}

	// If opened in append mode, write to the end of the file
	if o.OpenFlags&os.O_APPEND != 0 {
		o.readPos = len(o.Data)
	}

	o.Data = append(o.Data[:o.readPos], []byte(s)...)

	return len(s), nil
}
