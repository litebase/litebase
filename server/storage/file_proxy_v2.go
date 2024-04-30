package storage

import (
	"fmt"
	"io"
	internalStorage "litebasedb/internal/storage"
	"os"
	"sync"

	"github.com/klauspost/compress/s2"
)

type FileProxyV2 struct {
	exists    bool
	pageCache *PageCache
	mutex     *sync.Mutex
	pageSize  int64
	path      string
}

func NewFileProxyV2(path, databaseUuid, branchUuid string, pageSize int64) *FileProxyV2 {
	fp := &FileProxyV2{
		mutex:     &sync.Mutex{},
		pageCache: NewPageCache("TODO", databaseUuid, branchUuid, pageSize),
		path:      path,
	}

	return fp
}

func (fp *FileProxyV2) Open(path string) (internalStorage.File, error) {
	return nil, nil
}

func (fp *FileProxyV2) PageCache() *PageCache {
	return fp.pageCache
}

func (fp *FileProxyV2) ReadAt(offset int64) (data []byte, err error) {
	data, err = fp.pageCache.Get(offset)

	if err != nil {
		return nil, err
	}

	if len(data) > 0 {
		return data, nil
	}

	pageNumber := PageNumber(offset, fp.pageSize)

	fileData, err := FS().ReadFile(fp.pagePath(pageNumber))

	if err != nil {
		if os.IsNotExist(err) {
			return nil, io.EOF
		}

		return nil, err
	}

	decompressedData, err := decompressData(fileData)

	if err != nil {
		return nil, err
	}

	if pageNumber == 1 {
		fp.exists = true
	}

	// TODO: Devise a better caching strategy. Currently we are only caching spilled pages in P2
	// if err == nil && len(decompressedData) == int(fp.pageSize) {
	// 	fp.pageCache.Put(offset, decompressedData)
	// }

	return decompressedData, nil
}

func (fp *FileProxyV2) WriteAt(data []byte, offset int64) (n int, err error) {
	pageNumber := PageNumber(offset, fp.pageSize)

	if pageNumber == 1 && !fp.exists {
		fp.exists = true
	}

	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()

	compressedData, err := compressData(data)

	if err != nil {
		return 0, err
	}

	err = FS().WriteFile(fp.pagePath(pageNumber), compressedData, 0666)

	if err == nil {
		fp.pageCache.Put(offset, data)
	}

	n = len(data)

	data = nil

	return n, err
}

func (fp *FileProxyV2) WritePages(pages []struct {
	Data   []byte
	Length int64
	Offset int64
}) error {
	fp.mutex.Lock()
	defer fp.mutex.Unlock()

	// TODO: Batch writes
	// TODO: Lock for atomicity
	for _, page := range pages {
		_, err := fp.WriteAt(page.Data, page.Offset)

		if err != nil {
			return err
		}
	}

	return nil
}

func (fp *FileProxyV2) Size() (int64, error) {
	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()
	if fp.exists {
		return fp.pageSize * 4294967294, nil
	}

	return 0 * fp.pageSize, nil
}

// func PageNumber(offset, pageSize int64) int64 {
// 	return (offset / pageSize) + 1
// }

// func PageOffset(pagenumber, offset, pageSize int64) int64 {
// 	return offset - ((pagenumber - 1) * pageSize)
// }

func (fp *FileProxyV2) pagePath(pageNumber int64) string {
	return fmt.Sprintf("%s/page-%d", fp.path, pageNumber)
}

func compressData(data []byte) ([]byte, error) {
	return s2.Encode(nil, data), nil
}

func decompressData(data []byte) ([]byte, error) {
	return s2.Decode(nil, data)
}
