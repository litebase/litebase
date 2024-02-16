package file

import (
	"crypto/sha1"
	"fmt"
	"io"
	"litebasedb/internal/config"
	"litebasedb/server/storage"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

type PageCache struct {
	branchUuid    string
	databaseUuid  string
	file          *os.File
	directoryPath string
	freeList      []int64
	fs            *storage.FileSystem
	maxEntries    int
	mutex         *sync.RWMutex
	index         map[int64][]int64
	fileLock      *sync.Mutex
	syncCounter   int
	syncTicker    *time.Ticker
	syncThreshold int
	syncTime      time.Duration
}

func NewPageCache(databaseUuid string, branchUuid string) *PageCache {
	pc := &PageCache{
		branchUuid:    branchUuid,
		databaseUuid:  databaseUuid,
		directoryPath: fmt.Sprintf("%s/%s", config.Get().TmpPath, "page_cache"),
		fileLock:      &sync.Mutex{},
		freeList:      make([]int64, 0),
		fs:            storage.NewFileSystem("local"),
		index:         make(map[int64][]int64),
		maxEntries:    25000, // ? MB
		mutex:         &sync.RWMutex{},
		syncCounter:   0,
		syncThreshold: 0,
		syncTime:      10 * time.Millisecond,
		syncTicker:    time.NewTicker(250 * time.Millisecond),
	}

	go func() {
		for range pc.syncTicker.C {
			pc.Sync()
		}
	}()

	// Delete the directory if it exists
	err := pc.Clear()

	if err != nil {
		log.Println("ERROR CREATING PAGE CACHE DIRECTORY", err)
	}

	// Create the directory
	err = pc.fs.MkdirAll(pc.directoryPath, 0755)

	if err != nil {
		log.Println("ERROR CREATING PAGE CACHE DIRECTORY", err)
	}

	pc.file, err = os.OpenFile(pc.filePath(), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)

	if err != nil {
		log.Println("ERROR OPENING PAGE CACHE FILE", err)
	}

	return pc
}

func (pc *PageCache) Clear() error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.file.Close()
	pc.fs.RemoveAll(pc.directoryPath)
	pc.index = make(map[int64][]int64)

	return nil
}

func (pc *PageCache) filePath() string {
	hash := sha1.New()
	io.WriteString(hash, fmt.Sprintf("%s:%s", pc.databaseUuid, pc.branchUuid))
	hashString := fmt.Sprintf("%x", hash.Sum(nil))

	return fmt.Sprintf("%s/%s", pc.directoryPath, hashString)
}

func (pc *PageCache) Get(off int64) ([]byte, error) {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	pageNumer := PageNumber(off)

	if !pc.Has(off) {
		return nil, nil
	}

	// Get the page index
	entry, ok := pc.index[pageNumer]

	if !ok {
		return nil, nil
	}

	page := make([]byte, config.Get().PageSize)

	// Read the page from the file system
	pc.file.Seek(int64(entry[0]), 0)

	n, err := pc.file.Read(page)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	if n != int(config.Get().PageSize) {
		log.Println("ERROR READING PAGE", pageNumer, "NOT ENOUGH DATA")
		return nil, fmt.Errorf("page %d not enough data", pageNumer)
	}

	pageOffset := PageOffset(pageNumer, off)

	if pageOffset >= int64(len(page)) {
		return nil, fmt.Errorf("page offset %d out of bounds for page %d", pageOffset, PageNumber(off))
	}

	pc.index[PageNumber(off)] = []int64{
		entry[0],
		entry[1] + 1,
	}

	return page[pageOffset:], nil
}

func (pc *PageCache) Has(off int64) bool {
	pc.mutex.RLock()
	defer pc.mutex.RUnlock()
	_, ok := pc.index[PageNumber(off)]

	return ok
}

func (pc *PageCache) Put(off int64, p []byte) {
	pc.mutex.Lock()
	pageNumber := PageNumber(off)
	var err error
	offset := int64(0)

	// Check if there is a free page in the cache
	if len(pc.freeList) > 0 {
		offset = pc.freeList[0]
		pc.freeList = pc.freeList[1:]
		offset, err = pc.file.Seek(offset, 0)
	} else {
		offset, err = pc.file.Seek(offset, io.SeekEnd)
	}

	if err != nil {
		log.Println("ERROR SEEKING TO END OF PAGE CACHE FILE", err)
	}

	// Write the page to the file system
	pc.fileLock.Lock()

	n, err := pc.file.WriteAt(p, offset)

	pc.fileLock.Unlock()

	if err != nil {
		log.Printf("ERROR WRITING PAGE %d: %s", pageNumber, err)
	}

	if n != len(p) {
		log.Println("ERROR: NOT ALL DATA WAS WRITTEN TO PAGE CACHE FILE")
	}

	pc.syncCounter += 1

	if pc.syncCounter == 1000 {
		pc.Sync()
	}

	pc.index[pageNumber] = []int64{offset, 0}

	pc.mutex.Unlock()

	pc.Evict()
}

func (pc *PageCache) Delete(off int64) (err error) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()

	if !pc.Has(off) {
		return nil
	}

	// Get the cache offset
	entry := pc.index[PageNumber(off)]

	// Seek to the cache offset
	_, err = pc.file.Seek(entry[0], 0)

	pc.freeList = append(pc.freeList, entry[0])

	// Remove the page from the index
	delete(pc.index, PageNumber(off))

	// optionally delete the content in the file

	return nil
}

func (pc *PageCache) Evict() (err error) {
	pc.mutex.RLock()
	pageCount := len(pc.index)
	shouldEvict := pageCount > pc.maxEntries
	pc.mutex.RUnlock()

	if !shouldEvict {
		return nil
	}

	// pc.mutex.Lock()
	// defer pc.mutex.Unlock()

	pagesToEvict := pageCount - pc.maxEntries

	// Sort usage keys by value in ascending order
	keys := make([]int64, 0, pageCount)

	for k := range pc.index {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return pc.index[keys[i]][0] < pc.index[keys[j]][0]
	})

	evictedPages := 0

	for i := 0; i < pagesToEvict; i++ {
		err = pc.Delete(keys[i])

		if err != nil {
			log.Println("ERROR EVICTING PAGE", keys[i], err)
		}

		evictedPages += 1
	}

	log.Printf("EVICTED %d PAGES", evictedPages)

	return nil
}

func (pc *PageCache) Sync() {
	pc.fileLock.Lock()
	if pc.syncCounter > 0 {
		err := pc.file.Sync()

		if err != nil {
			log.Println("ERROR SYNCING PAGE CACHE FILE", err)
		}

		err = pc.file.Close()

		if err != nil {
			log.Println("ERROR CLOSING PAGE CACHE FILE", err)
		}

		pc.file, err = os.OpenFile(pc.filePath(), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)

		if err != nil {
			log.Println("ERROR OPENING PAGE CACHE FILE", err)
		}

		pc.syncCounter = 0
	}
	pc.fileLock.Unlock()
}
