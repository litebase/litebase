package storage

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"slices"
	"time"

	"github.com/klauspost/compress/s2"
)

type ObjectFile struct {
	client         *S3Client
	Data           []byte
	FileInfo       StaticFileInfo
	Key            string
	OpenFlags      int
	readPos        int
	Sha256Checksum [32]byte
}

func NewObjectFile(client *S3Client, key string, openFlags int, preExists bool) (*ObjectFile, error) {
	file := &ObjectFile{
		client: client,
		Data:   nil,
		FileInfo: StaticFileInfo{
			StaticName:    key,
			StaticSize:    0,
			StaticModTime: time.Now(),
		},
		Key:            key,
		OpenFlags:      openFlags,
		Sha256Checksum: sha256.Sum256([]byte{}),
	}

	fileExists := false

	if (openFlags&os.O_CREATE != 0) && preExists {
		fileExists = true
	} else if openFlags&os.O_CREATE != 0 {
		response, err := client.HeadObject(key)

		if err != nil {
			if response.StatusCode != 404 {
				log.Println("Error checking file existence", err)
				return nil, err
			}
		}

		if response.StatusCode == 200 {
			fileExists = true
		}

		if !fileExists {
			_, err = client.PutObject(key, []byte{})

			if err != nil {
				log.Println("Error creating file", err)
				return nil, err
			}
		}
	}

	if openFlags&os.O_RDONLY != 0 || openFlags&os.O_RDWR != 0 {
		response, err := client.GetObject(key)

		if err != nil {

			if response.StatusCode != 404 {
				return nil, err
			}
		}

		if len(response.Data) != 0 {
			file.Data, err = s2.Decode(nil, response.Data)

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
func (o *ObjectFile) Close() error {
	if len(o.Data) == 0 {
		return nil
	}

	if o.Sha256Checksum == sha256.Sum256(o.Data) {
		return nil
	}

	// Fail silently if the file is read-only
	if o.OpenFlags == os.O_RDONLY {
		return nil
	}

	compressed := s2.Encode(nil, o.Data)

	_, err := o.client.PutObject(o.Key, compressed)

	if err != nil {
		log.Println("Error closing file", err)
		return err
	}

	return nil
}

// Read bytes from the file.
func (o *ObjectFile) Read(p []byte) (n int, err error) {
	if o.Data == nil {
		response, err := o.client.GetObject(o.Key)

		if err != nil {
			return 0, err
		}

		if len(response.Data) == 0 {
			return 0, io.EOF
		}

		o.Data, err = s2.Decode(nil, response.Data)

		if err != nil {
			log.Println("Error decoding file", err)
			return 0, err
		}

		// Reset read position after fetching new data
		o.readPos = 0
	}

	n = copy(p, o.Data[o.readPos:])

	o.readPos += n

	if o.readPos >= len(o.Data) {
		err = io.EOF
	}

	return n, err
}

// Read bytes from the file at a specific offset.
func (o *ObjectFile) ReadAt(p []byte, off int64) (n int, err error) {
	if len(o.Data) == 0 {
		return 0, io.EOF
	}

	if off > int64(len(o.Data)) {
		return 0, io.EOF
	}

	n = copy(p, o.Data[off:])

	return n, nil
}

func (o *ObjectFile) Seek(offset int64, whence int) (int64, error) {
	if len(o.Data) == 0 {
		return 0, io.EOF
	}

	switch whence {
	case io.SeekStart:
		if offset < 0 || offset > int64(len(o.Data)) {
			return 0, io.EOF
		}

		o.readPos = int(offset)

		return offset, nil
	case io.SeekCurrent:
		newPos := int64(o.readPos) + offset

		if newPos < 0 || newPos > int64(len(o.Data)) {
			return 0, io.EOF
		}

		o.readPos = int(newPos)

		return newPos, nil
	case io.SeekEnd:
		newPos := int64(len(o.Data)) + offset

		if newPos < 0 || newPos > int64(len(o.Data)) {
			return 0, io.EOF
		}

		o.readPos = int(newPos)

		return newPos, nil
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
}

// Return stats about the file.
func (o *ObjectFile) Stat() (fs.FileInfo, error) {
	return o.FileInfo, nil
}

// Sync the file with the object store.
func (o *ObjectFile) Sync() error {
	if o.OpenFlags == os.O_RDONLY {
		return os.ErrPermission
	}

	compressed := s2.Encode(nil, o.Data)

	_, err := o.client.PutObject(o.Key, compressed)

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

	_, err := o.client.PutObject(o.Key, compressed)

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
