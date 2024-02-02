package file

import (
	"fmt"
	"io"
	"litebasedb/internal/config"
	internalStorage "litebasedb/internal/storage"
	"litebasedb/server/storage"
	"os"

	"github.com/klauspost/compress/zstd"
)

type FileProxyV2 struct {
	exists    bool
	pageCache *PageCache
	// mutex *sync.Mutex
	path string
}

func NewFileProxyV2(path string) *FileProxyV2 {
	// file, err := storage.FS().OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)

	// if err != nil {
	// 	panic(err)
	// }

	fp := &FileProxyV2{
		// file: file,
		// mutex: &sync.Mutex{},
		pageCache: NewPageCache(),
		path:      path,
	}

	return fp
}

func (fp *FileProxyV2) Open(path string) (internalStorage.File, error) {
	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()
	return nil, nil
}

func (fp *FileProxyV2) ReadAt(data []byte, offset int64) (n int, err error) {
	pageNumber := PageNumber(offset)

	if fp.pageCache.Has(offset) {
		n, err = fp.pageCache.ReadAt(data, offset)

		return n, err
	}

	fileData, err := storage.FS().ReadFile(fp.pagePath(pageNumber))

	// log.Println("READ PAGE", pageNumber, len(fileData))

	if err != nil {
		if os.IsNotExist(err) {
			return 0, io.EOF
		}

		return 0, err
	}

	decompressedData, err := decompressData(fileData)

	if err != nil {
		return 0, err
	}

	if pageNumber == 1 {
		fp.exists = true
	}

	n = copy(data, decompressedData)

	if err == nil && n == 4096 {
		fp.pageCache.WriteAt(decompressedData, offset)
	}

	return n, nil
}

func (fp *FileProxyV2) WriteAt(data []byte, offset int64) (n int, err error) {
	pageNumber := PageNumber(offset)

	if pageNumber == 1 {
		fp.exists = true
	}

	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()

	compressedData, err := compressData(data)

	if err != nil {
		return 0, err
	}

	// TODO: What if the write is less than the page size?
	err = storage.FS().WriteFile(fp.pagePath(pageNumber), compressedData, 0666)

	if err == nil {
		fp.pageCache.WriteAt(data, offset)
	}

	return len(data), err
}

func (fp *FileProxyV2) WritePages(pages []struct {
	Data   []byte
	Length int64
	Offset int64
}) error {
	// fp.mutex.Lock()
	// defer fp.mutex.Unlock()

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
		return 4096 * 4294967294, nil
	}

	return 0 * 4096, nil
}

func PageNumber(offset int64) int64 {
	return (offset / config.Get().PageSize) + 1
}

func PageOffset(pagenumber, offset int64) int64 {
	return offset - ((pagenumber - 1) * config.Get().PageSize)
}

func (fp *FileProxyV2) pagePath(pageNumber int64) string {
	return fmt.Sprintf("%s/page-%d", fp.path, pageNumber)
}

func compressData(data []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))

	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %v", err)
	}

	defer encoder.Close()

	compressedData := encoder.
		EncodeAll(data, make([]byte, 0))

	return compressedData, nil
}

func decompressData(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)

	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %v", err)
	}

	defer decoder.Close()

	return decoder.DecodeAll(data, make([]byte, 0))
}
