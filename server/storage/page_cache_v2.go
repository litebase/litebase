package storage

// import (
// 	"fmt"
// 	"log"
// 	"os"
// 	"sync"
// )

// type PageCache struct {
// 	branchUuid    string
// 	databaseUuid  string
// 	files         map[int64]*os.File
// 	directoryPath string
// 	fs            *FileSystem
// 	maxEntries    int
// 	mutex         *sync.RWMutex
// 	// index         map[int64][]int64
// 	pageSize int64
// 	// syncCounter   int
// 	// syncTicker    *time.Ticker
// 	// syncThreshold int
// 	// syncTime      time.Duration
// }

// func NewPageCache(
// 	tmpPath string,
// 	databaseUuid string,
// 	branchUuid string,
// 	pageSize int64,
// ) *PageCache {
// 	log.Println("NEW PAGE CACHE")
// 	pc := &PageCache{
// 		branchUuid:    branchUuid,
// 		databaseUuid:  databaseUuid,
// 		directoryPath: fmt.Sprintf("%s/%s", tmpPath, "page_cache"),
// 		fs:            NewFileSystem("local"),
// 		// index:         make(map[int64][]int64),
// 		maxEntries: 20000, // ? MB
// 		mutex:      &sync.RWMutex{},
// 		pageSize:   pageSize,
// 		// syncCounter:   0,
// 		// syncThreshold: 0,
// 		// syncTime:      10 * time.Millisecond,
// 		// syncTicker:    time.NewTicker(250 * time.Millisecond),
// 	}

// 	// go func() {
// 	// 	for range pc.syncTicker.C {
// 	// 		// pc.Sync()
// 	// 	}
// 	// }()

// 	// Delete the directory if it exists
// 	err := pc.Clear()

// 	if err != nil {
// 		log.Println("ERROR CREATING PAGE CACHE DIRECTORY", err)
// 	}

// 	// Create the directory
// 	err = pc.fs.MkdirAll(pc.directoryPath, 0755)

// 	if err != nil {
// 		log.Println("ERROR CREATING PAGE CACHE DIRECTORY", err)
// 	}

// 	return pc
// }

// func (pc *PageCache) Clear() error {
// 	pc.mutex.Lock()
// 	defer pc.mutex.Unlock()

// 	for _, file := range pc.files {
// 		file.Close()
// 	}
// 	pc.fs.RemoveAll(pc.directoryPath)
// 	pc.files = make(map[int64]*os.File)

// 	return nil
// }

// func (pc *PageCache) filePath(pageNumber int64) string {
// 	return fmt.Sprintf("%s/%d", pc.directoryPath, pageNumber)
// }

// func (pc *PageCache) Get(off int64) ([]byte, error) {
// 	pageNumber := PageNumber(off, pc.pageSize)

// 	pc.mutex.RLock()
// 	defer pc.mutex.RUnlock()

// 	// entry, ok := pc.index[pageNumber]

// 	// if !ok {
// 	// 	return nil, nil
// 	// }

// 	page := make([]byte, pc.pageSize)

// 	if pc.files[pageNumber] == nil {
// 		return nil, nil
// 	}

// 	// Read the page from the file system
// 	pc.files[pageNumber].Seek(0, 0)

// 	n, err := pc.files[pageNumber].Read(page)

// 	if err != nil {
// 		return nil, err
// 	}

// 	if n != int(pc.pageSize) {
// 		log.Println("ERROR READING PAGE", pageNumber, "NOT ENOUGH DATA")
// 		return nil, fmt.Errorf("page %d not enough data", pageNumber)
// 	}

// 	pageOffset := PageOffset(off, pc.pageSize)

// 	if pageOffset >= int64(len(page)) {
// 		return nil, fmt.Errorf("page offset %d out of bounds for page %d", pageOffset, PageNumber(off, pc.pageSize))
// 	}

// 	// pc.mutex.Lock()
// 	// pc.index[pageNumber] = []int64{
// 	// 	entry[0],
// 	// 	entry[1] + 1,
// 	// }
// 	// pc.mutex.Unlock()

// 	return page[pageOffset:], nil
// }

// func (pc *PageCache) Has(off int64) bool {
// 	pc.mutex.RLock()
// 	_, ok := pc.files[PageNumber(off, pc.pageSize)]
// 	pc.mutex.RUnlock()

// 	return ok
// }

// func (pc *PageCache) Put(off int64, p []byte) error {
// 	pageNumber := PageNumber(off, pc.pageSize)

// 	var err error
// 	offset := int64(0)

// 	if pc.files[pageNumber] == nil {
// 		pc.files[pageNumber], err = os.OpenFile(pc.filePath(pageNumber), os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)

// 		if err != nil {
// 			log.Println("ERROR OPENING PAGE CACHE FILE", err)
// 			return err
// 		}
// 	}

// 	pc.files[pageNumber].Seek(0, 0)

// 	n, err := pc.files[pageNumber].WriteAt(p, offset)

// 	if err != nil {
// 		log.Printf("ERROR WRITING PAGE %d: %s", pageNumber, err)
// 	}

// 	if n != len(p) {
// 		log.Println("ERROR: NOT ALL DATA WAS WRITTEN TO PAGE CACHE FILE")
// 		return fmt.Errorf("not all data was written to page cache file")
// 	}

// 	// pc.syncCounter += 1

// 	// if pc.syncCounter == 1000 {
// 	// 	pc.Sync()
// 	// }

// 	// return pc.Evict()
// 	return nil
// }

// func (pc *PageCache) Delete(off int64) (err error) {
// 	pc.mutex.Lock()
// 	defer pc.mutex.Unlock()

// 	return pc.remove(off)
// }

// // func (pc *PageCache) Evict() (err error) {
// // 	pc.mutex.RLock()
// // 	pageCount := len(pc.index)
// // 	shouldEvict := pageCount > pc.maxEntries
// // 	pc.mutex.RUnlock()

// // 	if !shouldEvict {
// // 		return nil
// // 	}

// // 	pc.mutex.Lock()
// // 	defer pc.mutex.Unlock()

// // 	pagesToEvict := pageCount - pc.maxEntries

// // 	// Sort usage keys by value in ascending order
// // 	keys := make([]int64, 0, pageCount)

// // 	for k := range pc.index {
// // 		keys = append(keys, k)
// // 	}

// // 	sort.Slice(keys, func(i, j int) bool {
// // 		return pc.index[keys[i]][0] < pc.index[keys[j]][0]
// // 	})

// // 	evictedPages := 0

// // 	for i := 0; i < pagesToEvict; i++ {
// // 		err = pc.remove(keys[i])

// // 		if err != nil {
// // 			log.Println("ERROR EVICTING PAGE", keys[i], err)
// // 		}

// // 		evictedPages += 1
// // 	}

// // 	log.Printf("EVICTED %d PAGES", evictedPages)

// // 	return nil
// // }

// func (pc *PageCache) remove(off int64) error {
// 	pageNumber := PageNumber(off, pc.pageSize)

// 	if pc.files[pageNumber] != nil {
// 		pc.files[pageNumber].Close()
// 		pc.files[pageNumber] = nil
// 		delete(pc.files, pageNumber)
// 	}

// 	return nil
// }
