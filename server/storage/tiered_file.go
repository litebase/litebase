package storage

import (
	"container/list"
	"io"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"os"
	"time"
)

type TieredFile struct {
	/*
		Closed is a boolean value that determines if the File has been Closed
		by local storage. If the File has been Closed, the File will be marked
		for release, which means the File will be removed from local storage
		and the TieredFileSystemDriver will no longer be able to access it.
	*/
	Closed bool
	/*
		CreatedAt stores the time the TieredFile struct was creeated. This
		value will be used in correlation with the updatedAt and writtenAt
		values to determine how long the File has been open.
	*/
	CreatedAt time.Time
	/*
		Element is a pointer to the list.Element that is used to store the File
		in the LRU cache. The Element is used to determine the position of the
		File in the LRU cache and to remove the File from the LRU cache.
	*/
	Element *list.Element
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

	f.tieredFileSystemDriver.WithLock(func() {
		f.tieredFileSystemDriver.ReleaseFile(f)
	})

	return nil
}

func (f *TieredFile) closeFile() error {
	f.Closed = true

	if f.File == nil {
		return nil
	}

	return f.File.Close()
}

func (f *TieredFile) MarkUpdated() {
	if f.Closed {
		return
	}

	f.UpdatedAt = time.Now()

	if f.Element != nil {
		f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)
	}
}

func (f *TieredFile) Read(p []byte) (n int, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.Read(p)
	}

	f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.Read(p)
}

func (f *TieredFile) ReadAt(p []byte, off int64) (n int, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.ReadAt(p, off)
	}

	f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.ReadAt(p, off)
}

func (f *TieredFile) Seek(offset int64, whence int) (n int64, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.Seek(offset, whence)
	}

	return f.File.Seek(offset, whence)
}

/*
This operation checks if the File should be written to durable storage. The File
should be written to durable storage if it has been updated and the last write
to durable storage was more than a minute ago.
*/
func (f *TieredFile) shouldBeWrittenToDurableStorage() bool {
	return f.UpdatedAt.After(f.WrittenAt) && time.Since(f.WrittenAt) >= f.tieredFileSystemDriver.WriteInterval
}

func (f *TieredFile) Stat() (fs.FileInfo, error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return nil, err
		}

		return file.Stat()
	}

	f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	return f.File.Stat()
}

func (f *TieredFile) Sync() error {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return err
		}

		return file.Sync()
	}

	err := f.File.Sync()

	if err != nil {
		return err
	}

	f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	f.UpdatedAt = time.Now()

	return nil
}

func (f *TieredFile) Truncate(size int64) error {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return err
		}

		return file.Truncate(size)
	}

	err := f.File.Truncate(size)

	if err != nil {
		return err
	}

	f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

	f.MarkUpdated()

	return nil
}

func (f *TieredFile) Write(p []byte) (n int, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.Write(p)
	}

	if f.Flag&os.O_RDONLY != 0 {
		return 0, fs.ErrInvalid
	}

	if f.Flag&os.O_WRONLY == 0 && f.Flag&os.O_RDWR == 0 {
		return 0, fs.ErrInvalid
	}

	n, err = f.File.Write(p)

	if err == nil {
		f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

		f.MarkUpdated()
	}

	return n, err
}

func (f *TieredFile) WriteAt(p []byte, off int64) (n int, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.WriteAt(p, off)
	}

	n, err = f.File.WriteAt(p, off)

	if err == nil {
		f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

		f.MarkUpdated()
	}

	return n, err
}

func (f *TieredFile) WriteTo(w io.Writer) (n int64, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.WriteTo(w)
	}

	n, err = f.File.WriteTo(w)

	if err == nil {
		f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

		f.MarkUpdated()
	}

	return n, err
}

func (f *TieredFile) WriteString(s string) (n int, err error) {
	if f.Closed {
		file, err := f.tieredFileSystemDriver.OpenFile(f.Key, f.Flag, 0644)

		if err != nil {
			return 0, err
		}

		return file.WriteString(s)
	}

	n, err = f.File.WriteString(s)

	if err == nil {
		f.tieredFileSystemDriver.FileOrder.MoveToBack(f.Element)

		f.MarkUpdated()
	}

	return n, err
}
