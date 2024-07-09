package storage

import (
	"context"
	"errors"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"net/http"
	"os"
	"sync"
)

type RemoteFileSystemDriver struct {
	connection *StorageConnection
	client     *http.Client
	// hasPageOne bool
	mutex *sync.RWMutex
	// pageCache    *PageCache
	// pageSize int64
	// size     int64
}

func NewRemoteFileSystemDriver(ctx context.Context) *RemoteFileSystemDriver {
	fs := &RemoteFileSystemDriver{
		connection: StorageConnectionManager().Create(ctx, "http://localhost:8085"),
		client:     &http.Client{},
		// hasPageOne: false,
		mutex: &sync.RWMutex{},
		// pageCache:    NewPageCache(tmpPath, databaseUuid, branchUuid, pageSize),
		// pageSize: pageSize,
		// size:     0,
	}

	return fs
}

func (fs *RemoteFileSystemDriver) Create(file string) (internalStorage.File, error) {
	log.Fatalln("Not implemented")
	return nil, nil
}

// func (fs *RemoteFileSystemDriver) Delete(file string) error {
// 	url := fmt.Sprintf("%s/databases/%s/%s/%s", getStorageUrl(), fs.databaseUuid, fs.branchUuid, file)

// 	request, err := http.NewRequest("DELETE", url, nil)

// 	if err != nil {
// 		return err
// 	}

// 	_, err = fs.client.Do(request)

// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (fs *RemoteFileSystemDriver) getFileSize() {
// 	if fs.hasPageOne {
// 		fs.size = fs.pageSize * 4294967294
// 		return
// 	}

// 	fs.size = 0 * fs.pageSize
// }

func (fs *RemoteFileSystemDriver) Mkdir(path string, perm fs.FileMode) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandMkdir,
		Path:    path,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (fs *RemoteFileSystemDriver) MkdirAll(path string, perm fs.FileMode) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandMkdirAll,
		Path:    path,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (fs *RemoteFileSystemDriver) Open(path string) (internalStorage.File, error) {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandOpen,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	return NewRemoteFile(fs, path, response.FileId), nil
}

func (fs *RemoteFileSystemDriver) OpenFile(path string, flag int, perm fs.FileMode) (internalStorage.File, error) {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandOpenFile,
		Path:    path,
		Flag:    flag,
		Perm:    perm,
	})

	if err != nil {
		return nil, err
	}

	if response.Error != "" {
		if !response.Exists {
			return nil, os.ErrNotExist
		}

		return nil, errors.New(response.Error)
	}

	return NewRemoteFile(fs, path, response.FileId), nil
}

func (fs *RemoteFileSystemDriver) ReadDir(path string) ([]internalStorage.DirEntry, error) {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandReadDir,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	if response.Error != "" {
		if !response.Exists {
			return nil, os.ErrNotExist
		}

		return nil, errors.New(response.Error)
	}

	return response.DirEntries, nil
}

func (fs *RemoteFileSystemDriver) ReadFile(path string) ([]byte, error) {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandReadFile,
		Path:    path,
	})

	if err != nil {
		return nil, err
	}

	if response.Error != "" {
		if !response.Exists {
			return nil, os.ErrNotExist
		}

		return nil, errors.New(response.Error)
	}

	return response.Data, nil
}

func (fs *RemoteFileSystemDriver) Remove(path string) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandRemove,
		Path:    path,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		if !response.Exists {
			return os.ErrNotExist
		}

		return errors.New(response.Error)
	}

	return nil
}

func (fs *RemoteFileSystemDriver) RemoveAll(path string) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandRemoveAll,
		Path:    path,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		if !response.Exists {
			return os.ErrNotExist
		}

		return errors.New(response.Error)
	}

	return nil
}

func (fs *RemoteFileSystemDriver) Rename(oldpath, newpath string) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandRename,
		Path:    oldpath,
		Data:    []byte(newpath),
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		if !response.Exists {
			return os.ErrNotExist
		}

		return errors.New(response.Error)
	}

	return nil
}

func (fs *RemoteFileSystemDriver) Stat(path string) (internalStorage.FileInfo, error) {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandStat,
		Path:    path,
	})

	if err != nil {
		return internalStorage.FileInfo{}, err
	}

	if response.Error != "" {
		if !response.Exists {
			return internalStorage.FileInfo{}, os.ErrNotExist
		}

		return internalStorage.FileInfo{}, errors.New(response.Error)
	}

	return response.FileInfo, nil
}

func (fs *RemoteFileSystemDriver) Truncate(name string, size int64) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandTruncate,
		Path:    name,
		Size:    size,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		if !response.Exists {
			return os.ErrNotExist
		}

		return errors.New(response.Error)
	}

	return nil
}

func (fs *RemoteFileSystemDriver) WriteFile(path string, data []byte, perm fs.FileMode) error {
	response, err := fs.connection.Send(internalStorage.StorageRequest{
		Command: internalStorage.StorageCommandWriteFile,
		Data:    data,
		Path:    path,
		Perm:    perm,
	})

	if err != nil {
		return err
	}

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}
