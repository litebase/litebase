package storage

import (
	"io"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"os"
	"time"
)

type TieredFile struct {
	/*
		CreatedAt stores the time the TieredFile struct was creeated. This
		value will be used in correlation with the updatedAt and writtenAt
		values to determine how long the File has been open.
	*/
	CreatedAt time.Time
	/*
		Closed is a boolean value that determines if the File has been Closed
		by local storage. If the File has been Closed, the File will be marked
		for release, which means the File will be removed from local storage
		and the TieredFileSystemDriver will no longer be able to access it.
	*/
	Closed bool
	/*
		File is the internalStorage.File object that is used to read and write
		data to the File. The File is a instance of *os.File which points to a
		local File. If the File is nil, the File has not been opened yet.
	*/
	File internalStorage.File
	/*
		Flags that defined the file permissions used for access. This value will
		be used to determine if the File should be written to durable storage.
	*/
	Flag int
	/*
		Used to identify the File in the durable storage and local storage.
	*/
	Key string
	/*
		The pointer to the FileSystemDriver that created the File.
	*/
	tieredFileSystemDriver *TieredFileSystemDriver
	/*
		UpdatedAt stores the time the File was last updated. This value will be
		used in correlation with the CreatedAt and writtenAt values to determine
		how long the File has been open and if the File should be written to
		durable storage.
	*/
	UpdatedAt time.Time
	/*
		WrittenAt stores the time the File was last written to durable storage.
		This value will be used in correlation with the CreatedAt and updatedAt
		values to determine how long the File has been open and if the File
		should be written to durable storage.
	*/
	WrittenAt time.Time
}

func NewTieredFile(
	tieredFileSystemDriver *TieredFileSystemDriver,
	key string,
	file internalStorage.File,
	flag int,
) *TieredFile {
	return &TieredFile{
		CreatedAt:              time.Now(),
		File:                   file,
		Flag:                   flag,
		Key:                    key,
		tieredFileSystemDriver: tieredFileSystemDriver,
		UpdatedAt:              time.Time{},
		WrittenAt:              time.Time{},
	}
}

func (f *TieredFile) Close() error {
	if f.Closed {
		return nil
	}

	f.Closed = true

	if f.File == nil {
		return nil
	}

	err := f.File.Sync()

	if err != nil {
		return err
	}

	// if f.updatedAt.After(f.writtenAt) {
	// 	err := f.Sync()

	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return f.File.Close()
}

func (f *TieredFile) MarkUpdated() {
	f.UpdatedAt = time.Now()
}

func (f *TieredFile) Read(p []byte) (n int, err error) {
	if f.File == nil {
		// Pull the File from durable storage
		data, err := f.tieredFileSystemDriver.durableFileSystemDriver.ReadFile(f.Key)

		if err != nil {
			return 0, err
		}

		err = f.tieredFileSystemDriver.localFileSystemDriver.WriteFile(f.Key, data, 0)

		if err != nil {
			return 0, err
		}

		return copy(p, data), io.EOF
	}

	return f.File.Read(p)
}

func (f *TieredFile) ReadAt(p []byte, off int64) (n int, err error) {
	if f.File == nil {
		// Pull the File from durable storage
		data, err := f.tieredFileSystemDriver.durableFileSystemDriver.ReadFile(f.Key)

		if err != nil {
			return 0, err
		}

		err = f.tieredFileSystemDriver.localFileSystemDriver.WriteFile(f.Key, data, 0)

		if err != nil {
			return 0, err
		}

		return copy(p, data[off:]), nil
	}

	return f.File.ReadAt(p, off)
}

func (f *TieredFile) Seek(offset int64, whence int) (int64, error) {
	if f.File == nil {
		return 0, nil
	}

	return f.File.Seek(offset, whence)
}

/*
This operation checks if the File should be written to durable storage. The File
should be written to durable storage if it has been updated and the last write
to durable storage was more than a minute ago.
*/
func (f *TieredFile) shouldBeWrittenToDurableStorage() bool {
	return f.UpdatedAt.After(f.WrittenAt) && time.Since(f.WrittenAt) >= time.Second
}

func (f *TieredFile) Stat() (fs.FileInfo, error) {
	return f.File.Stat()
}

func (f *TieredFile) Sync() error {
	err := f.File.Sync()

	if err != nil {
		return err
	}

	f.UpdatedAt = time.Now()

	return nil
}

func (f *TieredFile) Truncate(size int64) error {
	err := f.File.Truncate(size)

	if err != nil {
		return err
	}

	f.UpdatedAt = time.Now()

	return nil
}

func (f *TieredFile) Write(p []byte) (n int, err error) {
	if f.Flag == os.O_RDONLY {
		return 0, fs.ErrInvalid
	}

	n, err = f.File.Write(p)

	if err == nil {
		f.UpdatedAt = time.Now()
	}

	return n, err
}

func (f *TieredFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = f.File.WriteAt(p, off)

	if err == nil {
		f.MarkUpdated()
	}

	return n, err
}

func (f *TieredFile) WriteTo(w io.Writer) (n int64, err error) {
	n, err = f.File.WriteTo(w)

	if err == nil {
		f.UpdatedAt = time.Now()
	}

	return n, err
}

func (f *TieredFile) WriteString(s string) (n int, err error) {
	n, err = f.File.WriteString(s)

	if err == nil {
		f.UpdatedAt = time.Now()
	}

	return n, err
}
