package database

import (
	timer "litebasedb/internal"
	"litebasedb/internal/config"
	internalStorage "litebasedb/internal/storage"
	"litebasedb/server/file"
	"log"
	"sort"
	"sync"
)

type FileSystem struct {
	checkpointer *Checkpointer
	computedSize int64
	mutex        *sync.RWMutex
	proxy        file.Proxy
	readBytes    int64
	path         string
	size         int64
	wal          *WAL
}

func NewFileSystem(path string) *FileSystem {
	fs := &FileSystem{
		mutex: &sync.RWMutex{},
		path:  path,
		proxy: file.NewFileProxyV2(path),
		size:  0,
		wal:   NewWAL(),
	}

	fs.checkpointer = NewCheckpointer(func() {
		fs.mutex.Lock()
		fs.CheckPoint()
		fs.mutex.Unlock()
	})

	return fs
}

func (fs *FileSystem) CheckPoint() error {
	var keys []int64

	for k := range fs.wal.pages {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	pages := make([]struct {
		Data   []byte
		Length int64
		Offset int64
	}, 0)

	for _, pageNumber := range keys {
		data := fs.wal.pages[pageNumber]
		offset := int64(pageNumber-1) * config.Get().PageSize

		page := struct {
			Data   []byte
			Length int64
			Offset int64
		}{
			Data:   data,
			Length: int64(len(data)),
			Offset: offset,
		}

		pages = append(pages, page)

		delete(fs.wal.pages, pageNumber)
	}

	if len(pages) == 0 {
		return nil
	}

	fs.proxy.WritePages(pages)

	log.Printf("CHECKPOINTED %d PAGES or %.1fMB", len(keys), float64(len(keys))*float64(config.Get().PageSize)/1024/1024)
	// log.Printf("READ %.1fMB", float64(fs.readBytes)/1024/1024)

	fs.getFileSize()

	return nil
}

func (fs *FileSystem) getFileSize() {
	size, err := fs.proxy.Size()

	if err != nil {
		log.Println(err)
	}

	fs.size = size
}

// func (fs *FileSystem) Has(pageNumber int64) bool {
// 	_, ok := fs.wal.pages[pageNumber]

// 	return ok
// }

func (fs *FileSystem) Open(path string) (internalStorage.File, error) {
	return fs.proxy.Open(path)
}

func (fs *FileSystem) ReadAt(data []byte, offset int64) (n int, err error) {
	start := timer.Start("READ PAGE")
	pageNumber := PageNumber(offset)
	pageOffset := PageOffset(offset)

	if fs.wal.Has(pageNumber) {
		n, err = fs.wal.Read(pageNumber, pageOffset, data)
	} else {
		n, err = fs.proxy.ReadAt(data, offset)

		fs.readBytes += int64(n)
	}

	timer.Stop(start)

	if err != nil {
		return 0, err
	}

	return n, err
}

func (fs *FileSystem) WriteAt(data []byte, offset int64) (n int, err error) {
	start := timer.Start("WRITE PAGE")
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	n, err = fs.wal.Write(PageNumber(offset), data)

	if len(fs.wal.pages) >= 1000 {
		fs.CheckPoint()
	}

	timer.Stop(start)

	return n, err
}

func (fs *FileSystem) Size() (int64, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	if fs.size == 0 {
		fs.getFileSize()
	}

	if fs.size == 0 && len(fs.wal.pages) > 1 {
		return int64(len(fs.wal.pages)) * config.Get().PageSize, nil
	}

	if fs.size > 0 && (len(fs.wal.pages) > 0) {
		maxPages := int64(fs.size / config.Get().PageSize)
		newPages := 0

		for pageNumber := range fs.wal.pages {
			if pageNumber > maxPages {
				newPages++
			}
		}

		totalPages := int64(newPages) + maxPages
		computedSize := totalPages * config.Get().PageSize
		fs.computedSize = computedSize

		return computedSize, nil
	}

	return fs.size, nil
}

func PageNumber(offset int64) int64 {
	return (offset / config.Get().PageSize) + 1
}

// Calculate the offset of the page within the file
func PageOffset(offset int64) int64 {
	pageNumber := PageNumber(offset)

	return offset - (int64(pageNumber-1) * config.Get().PageSize)
}
