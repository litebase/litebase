package storage

import (
	"io"
	"io/fs"
	internalStorage "litebase/internal/storage"
	"time"
)

type TieredFile struct {
	closed                 bool
	file                   internalStorage.File
	key                    string
	tieredFileSystemDriver *TieredFileSystemDriver
	updatedAt              time.Time
	writtenAt              time.Time
}

func NewTieredFile(
	tieredFileSystemDriver *TieredFileSystemDriver,
	key string,
	file internalStorage.File,
) *TieredFile {
	return &TieredFile{
		file:                   file,
		key:                    key,
		tieredFileSystemDriver: tieredFileSystemDriver,
		updatedAt:              time.Time{},
		writtenAt:              time.Time{},
	}
}

func (f *TieredFile) Close() error {
	if f.closed {
		return nil
	}

	f.closed = true

	if f.file == nil {
		return nil
	}

	err := f.file.Sync()

	if err != nil {
		return err
	}

	// if f.updatedAt.After(f.writtenAt) {
	// 	err := f.Sync()

	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return f.file.Close()
}

func (f *TieredFile) Read(p []byte) (n int, err error) {
	if f.file == nil {
		// Pull the file from object storage
		data, err := f.tieredFileSystemDriver.objectFileSystemDriver.ReadFile(f.key)

		if err != nil {
			return 0, err
		}

		err = f.tieredFileSystemDriver.localFileSystemDriver.WriteFile(f.key, data, 0)

		if err != nil {
			return 0, err
		}

		return copy(p, data), nil
	}

	return f.file.Read(p)
}

func (f *TieredFile) ReadAt(p []byte, off int64) (n int, err error) {
	if f.file == nil {
		// Pull the file from object storage
		data, err := f.tieredFileSystemDriver.objectFileSystemDriver.ReadFile(f.key)

		if err != nil {
			return 0, err
		}

		err = f.tieredFileSystemDriver.localFileSystemDriver.WriteFile(f.key, data, 0)

		if err != nil {
			return 0, err
		}

		return copy(p, data[off:]), nil
	}

	return f.file.ReadAt(p, off)
}

func (f *TieredFile) Seek(offset int64, whence int) (int64, error) {
	if f.file == nil {
		return 0, nil
	}

	return f.file.Seek(offset, whence)
}

/*
This operation checks if the file should be written to object storage. The file
should be written to object storage if it has been updated and the last write
to object storage was more than a minute ago.
*/
func (f *TieredFile) shouldBeWrittenToObjectStorage() bool {
	return f.updatedAt.After(f.writtenAt) && time.Since(f.writtenAt) > time.Second
}

func (f *TieredFile) Stat() (fs.FileInfo, error) {
	return f.file.Stat()
}

func (f *TieredFile) Sync() error {
	err := f.file.Sync()

	if err != nil {
		return err
	}

	f.updatedAt = time.Now()

	return nil
}

func (f *TieredFile) Truncate(size int64) error {
	err := f.file.Truncate(size)

	if err != nil {
		return err
	}

	f.updatedAt = time.Now()

	return nil
}

func (f *TieredFile) Write(p []byte) (n int, err error) {
	n, err = f.file.Write(p)

	if err == nil {
		f.updatedAt = time.Now()
	}

	return n, err
}

func (f *TieredFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = f.file.WriteAt(p, off)

	if err == nil {
		f.updatedAt = time.Now()
	}

	return n, err
}

func (f *TieredFile) WriteTo(w io.Writer) (n int64, err error) {
	n, err = f.file.WriteTo(w)

	if err == nil {
		f.updatedAt = time.Now()
	}

	return n, err
}

func (f *TieredFile) WriteString(s string) (n int, err error) {
	n, err = f.file.WriteString(s)

	if err == nil {
		f.updatedAt = time.Now()
	}

	return n, err
}
