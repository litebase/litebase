package file

import (
	"io/fs"
	internalStorage "litebasedb/internal/storage"
	"litebasedb/server/storage"
	"os"
)

type FileProxy struct {
	file internalStorage.File
	// mutex *sync.Mutex
	path string
}

func NewFileProxy(path string) *FileProxy {
	file, err := storage.FS().OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)

	if err != nil {
		panic(err)
	}

	fp := &FileProxy{
		file: file,
		// mutex: &sync.Mutex{},
		path: path,
	}

	return fp
}

func (fp *FileProxy) MkdirAll(path string, perm fs.FileMode) error {
	return storage.FS().MkdirAll(path, perm)
}

func (fp *FileProxy) ReadAt(data []byte, offset int64) (n int, err error) {
	return fp.file.ReadAt(data, offset)
}

func (fp *FileProxy) WriteAt(data []byte, offset int64) (n int, err error) {
	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()

	return fp.file.WriteAt(data, offset)
}

func (fp *FileProxy) WritePages(pages []struct {
	Data   []byte
	Length int64
	Offset int64
}) error {
	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()

	for _, page := range pages {
		_, err := fp.file.WriteAt(page.Data, page.Offset)

		if err != nil {
			return err
		}
	}

	return nil
}

func (fp *FileProxy) Size() (int64, error) {
	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()
	fileInfo, err := storage.FS().Stat(fp.path)

	if err != nil {
		return 0, err
	}

	return fileInfo.Size(), nil
}
