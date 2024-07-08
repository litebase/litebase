package storage

import (
	"errors"
	internalStorage "litebase/internal/storage"
	"log"
)

type RemoteFile struct {
	fs   *RemoteFileSystemDriver
	Id   string
	Path string
}

func NewRemoteFile(fs *RemoteFileSystemDriver, path, id string) *RemoteFile {
	return &RemoteFile{
		fs:   fs,
		Id:   id,
		Path: path,
	}
}

func (rf *RemoteFile) Close() error {
	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandFileClose,
		FileId:  rf.Id,
		Path:    rf.Path,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (rf *RemoteFile) Read(p []byte) (n int, err error) {
	log.Println("Reading file")
	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandReadFile,
		FileId:  rf.Id,
		Path:    rf.Path,
		Size:    int64(len(p)),
	})

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	n = copy(p, response.Data)

	return n, nil
}

func (rf *RemoteFile) ReadAt(p []byte, off int64) (n int, err error) {
	log.Println("Reading file at")

	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandReadFile,
		FileId:  rf.Id,
		Path:    rf.Path,
		Size:    int64(len(p)),
	})

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	n = copy(p, response.Data)

	return n, nil
}

func (rf *RemoteFile) Seek(offset int64, whence int) (int64, error) {
	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandFileSeek,
		FileId:  rf.Id,
		Offset:  offset,
		Path:    rf.Path,
		Whence:  whence,
	})

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	return response.Offset, nil
}

func (rf *RemoteFile) Write(p []byte) (n int, err error) {
	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandFileWrite,
		Data:    p,
		FileId:  rf.Id,
		Path:    rf.Path,
	})

	if err != nil {

		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	return response.Length, nil
}

func (rf *RemoteFile) WriteAt(p []byte, off int64) (n int, err error) {
	log.Println("Writing file at")

	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandFileWriteAt,
		Data:    p,
		FileId:  rf.Id,
		Offset:  off,
		Path:    rf.Path,
	})

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	return response.Length, nil
}

func (rf *RemoteFile) WriteString(s string) (ret int, err error) {
	log.Println("Writing string")

	response, err := rf.fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandFileWriteString,
		Data:    []byte(s),
		FileId:  rf.Id,
		Path:    rf.Path,
	})

	if err != nil {
		return 0, err
	}

	if response.Error != "" {
		return 0, errors.New(response.Error)
	}

	return response.Length, nil
}
