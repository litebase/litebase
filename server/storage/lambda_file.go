package storage

import (
	"errors"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

// LambdaFile is a file that is stored remotely. When the file is opened, it is
// downloaded from the remote storage provider and stored in memory. When the
// file is closed, it is removed from memory.
//
// When a file is opened the first chunk of the file is downloaded. If there are more
// chunks, they are downloaded as needed.
type LambdaFile struct {
	bytes      int64
	client     *lambda.Client
	data       []byte
	name       string
	totalBytes int64
}

func (f *LambdaFile) Close() error {
	// Clear the data from memory
	f.data = nil

	return nil
}

func (f *LambdaFile) Read(p []byte) (n int, err error) {
	if f.bytes == f.totalBytes {
		return copy(p, f.data), nil
	}

	if f.bytes < f.totalBytes {
		log.Fatal("TODO: download next chunk")
		// TODO: download next chunk
	}

	return 0, nil
}

func (f *LambdaFile) ReadAt(p []byte, off int64) (n int, err error) {
	response, err := remoteFileProcedure(
		f.client,
		NewFilesystemRequest("readat", f.name).
			WithData(p).
			WithOffset(off),
	)

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	if len(response.Data) == 0 {
		return 0, nil
	}

	return copy(p, response.Data), nil
}

func (f *LambdaFile) Write(p []byte) (n int, err error) {
	response, err := remoteFileProcedure(
		f.client,
		NewFilesystemRequest("write", f.name).
			WithData(p),
	)

	if err != nil {
		return 0, err
	}

	return int(response.Bytes), nil
}

func (f *LambdaFile) WriteAt(p []byte, off int64) (n int, err error) {
	response, err := remoteFileProcedure(
		f.client,
		NewFilesystemRequest("writeat", f.name).
			WithData(p).
			WithOffset(off),
	)

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	return int(response.Bytes), nil
}

func (f *LambdaFile) WriteString(s string) (ret int, err error) {
	response, err := remoteFileProcedure(
		f.client,
		NewFilesystemRequest("write", f.name).
			WithData([]byte(s)),
	)

	if err != nil {
		return 0, err
	}

	return int(response.Bytes), nil
}
