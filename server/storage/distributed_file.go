package storage

import (
	"container/list"
	"io"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"log"
	"sync"
)

/*
A DistributedFile represents a file that is opened on a remote storage node.
This file can be treated as a regular file, but all operations are performed
remotely on the storage node allowing for the server to operate without storing
and multiple query nodes to operate on the same file without consensus issues.
*/
type DistributedFile struct {
	distributedFileSystemDriver *DistributedFileSystemDriver
	/*
		Element is a pointer to the list.Element that is used to store the File
		in the LRU cache. The Element is used to determine the position of the
		File in the LRU cache and to remove the File from the LRU cache.
	*/
	Element *list.Element
	/*
		File is the internalStorage.File object that is used to read data from
		the File. The File is a instance of *os.File which points to a local
		File. If the File is nil, the File has not been opened yet.
	*/
	File internalStorage.File
	/*
		Flags that defined the file permissions used for access. This value will
		be used to determine if the File should be written to durable storage.
	*/
	Flag int
	/*
		Offset is the current offset of the file. This value is used to determine
		where the next read or write operation should occur. In case of a network
		failure, the offset will be used to determine where the next operation
		should occur from the last successful operation.
	*/
	Offset int64
	/*
		Mutex is a pointer to a sync.Mutex that is used to lock the file when
		reading or writing to the file. This is used to prevent multiple
		operations from occurring at the same time.
	*/
	mutex *sync.Mutex
	/*
		The file path that is used to identify the file on the storage node.
	*/
	Path string
	/*
		Ther permissions that were used to create or open the file.
	*/
	Perm fs.FileMode

	/*
		The storage connection manager that is used to send requests to the
		distributed storage nodes.
	*/
	storageConnectionManager *StorageConnectionManager
}

/*
Create a new instance of the DistributedFile.
*/
func NewDistributedFile(
	distributedFileSystemDriver *DistributedFileSystemDriver,
	path string,
	file internalStorage.File,
	flag int,
	perm fs.FileMode,
) *DistributedFile {
	return &DistributedFile{
		distributedFileSystemDriver: distributedFileSystemDriver,
		File:                        file,
		Flag:                        flag,
		mutex:                       &sync.Mutex{},
		Path:                        path,
		Perm:                        perm,
		storageConnectionManager:    distributedFileSystemDriver.storageConnectionManager,
	}
}

/*
Close the file.
*/
func (df *DistributedFile) Close() error {
	df.mutex.Lock()
	defer df.mutex.Unlock()
	defer df.distributedFileSystemDriver.ReleaseFile(df)

	_, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: CloseStorageCommand,
		Path:    df.Path,
	})

	if err != nil {
		return err
	}

	if df.File != nil {
		df.File.Close()
		df.File = nil
	}

	return nil
}

/*
Read from the file.
*/
func (df *DistributedFile) Read(p []byte) (n int, err error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		return df.File.Read(p)
	}

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: ReadStorageCommand,
		Flag:    df.Flag,
		Length:  len(p),
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return 0, err
	}

	n = copy(p, response.Data)

	return n, nil
}

/*
Read from the file at the specified offset.
*/
func (df *DistributedFile) ReadAt(p []byte, off int64) (n int, err error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		return df.File.ReadAt(p, off)
	}

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: ReadAtStorageCommand,
		Flag:    df.Flag,
		Length:  len(p),
		Offset:  off,
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return 0, err
	}

	n = copy(p, response.Data)

	return n, nil
}

/*
Seek to the specified offset.
*/
func (df *DistributedFile) Seek(offset int64, whence int) (int64, error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: SeekStorageCommand,
		Flag:    df.Flag,
		Offset:  offset,
		Path:    df.Path,
		Perm:    df.Perm,
		Whence:  whence,
	})

	if err != nil {
		log.Println("Error seeking", err)
		return 0, err
	}

	if df.File != nil {
		_, err = df.File.Seek(offset, whence)

		if err != nil {
			log.Println("Error seeking", err)
		}
	}

	df.Offset = response.Offset

	return response.Offset, nil
}

/*
Stat the file.
*/
func (df *DistributedFile) Stat() (fs.FileInfo, error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: StatFileStorageCommand,
		Flag:    df.Flag,
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return nil, err
	}

	return response.FileInfo, nil
}

/*
Sync the file.
*/
func (df *DistributedFile) Sync() error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	_, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: SyncStorageCommand,
		Flag:    df.Flag,
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return err
	}

	return nil
}

/*
Truncate the file to the specified size.
*/
func (df *DistributedFile) Truncate(size int64) error {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	_, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: TruncateFileStorageCommand,
		Flag:    df.Flag,
		Path:    df.Path,
		Perm:    df.Perm,
		Size:    size,
	})

	if err != nil {
		return err
	}

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		err := df.File.Truncate(size)

		if err != nil {
			df.distributedFileSystemDriver.ReleaseFile(df)
		}
	}

	df.Offset = size

	return nil
}

/*
Write to the file.
*/
func (df *DistributedFile) Write(p []byte) (n int, err error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: WriteStorageCommand,
		Data:    p,
		Flag:    df.Flag,
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return 0, err
	}

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		_, err := df.File.Write(p)

		if err != nil {
			df.distributedFileSystemDriver.ReleaseFile(df)
		}
	}

	df.Offset += int64(response.BytesProcessed)

	return response.BytesProcessed, nil
}

/*
Write to the file at the specified offset.
*/
func (df *DistributedFile) WriteAt(p []byte, off int64) (n int, err error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: WriteAtStorageCommand,
		Data:    p,
		Flag:    df.Flag,
		Offset:  off,
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return 0, err
	}

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		_, err := df.File.WriteAt(p, off)

		if err != nil {
			df.distributedFileSystemDriver.ReleaseFile(df)
		}
	}

	df.Offset += int64(response.BytesProcessed)

	return response.BytesProcessed, nil
}

/*
Write the file to the writer.
*/
func (df *DistributedFile) WriteTo(w io.Writer) (n int64, err error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	panic("Not implemented")

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: WriteToStorageCommand,
		Path:    df.Path,
	})

	if err != nil {
		return 0, err
	}

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		_, err := df.File.WriteTo(w)

		if err != nil {
			df.distributedFileSystemDriver.ReleaseFile(df)
		}
	}

	df.Offset += int64(response.BytesProcessed)

	return int64(response.BytesProcessed), nil
}

/*
Write a string to the file.
*/
func (df *DistributedFile) WriteString(s string) (ret int, err error) {
	df.mutex.Lock()
	defer df.mutex.Unlock()

	response, err := df.storageConnectionManager.Send(DistributedFileSystemRequest{
		Command: WriteStringStorageCommand,
		Data:    []byte(s),
		Flag:    df.Flag,
		Path:    df.Path,
		Perm:    df.Perm,
	})

	if err != nil {
		return 0, err
	}

	if df.File != nil {
		df.distributedFileSystemDriver.FileOrder.MoveToBack(df.Element)

		_, err := df.File.WriteString(s)

		if err != nil {
			df.distributedFileSystemDriver.ReleaseFile(df)
		}
	}

	return response.BytesProcessed, nil
}
