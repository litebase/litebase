package file

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

type PageCache struct {
	maxEntries int
	mutex      *sync.Mutex
	pages      map[int64][]byte
	usage      map[int64]int64
}

func NewPageCache() *PageCache {
	return &PageCache{
		maxEntries: 128000, // 128 MB
		mutex:      &sync.Mutex{},
		pages:      make(map[int64][]byte),
		usage:      make(map[int64]int64),
	}
}

func (pc *PageCache) Clear() error {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.pages = make(map[int64][]byte)
	pc.usage = make(map[int64]int64)

	return nil
}

func (pc *PageCache) ReadAt(data []byte, off int64) (int, error) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pageNumer := PageNumber(off)
	page, ok := pc.pages[PageNumber(off)]

	if !ok {
		return 0, fmt.Errorf("page %d not found in cache", pageNumer)
	}

	pageOffset := PageOffset(pageNumer, off)

	if pageOffset >= int64(len(page)) {
		return 0, fmt.Errorf("page offset %d out of bounds for page %d", pageOffset, PageNumber(off))
	}

	n := copy(data, page[pageOffset:])

	if n != len(data) {
		return n, fmt.Errorf("expected to copy %d bytes, but copied %d", len(data), n)
	}

	pc.usage[PageNumber(off)] += 1

	return n, nil
}

func (pc *PageCache) Has(off int64) bool {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	_, ok := pc.pages[PageNumber(off)]

	return ok
}

func (pc *PageCache) WriteAt(p []byte, off int64) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	pc.pages[PageNumber(off)] = p
	pc.usage[PageNumber(off)] = 1
	pc.Evict()
}

func (pc *PageCache) Delete(off int64) (err error) {
	pc.mutex.Lock()
	defer pc.mutex.Unlock()
	delete(pc.pages, PageNumber(off))
	delete(pc.usage, PageNumber(off))

	return nil
}

func (pc *PageCache) Evict() (err error) {
	pageCount := len(pc.pages)

	if pageCount <= pc.maxEntries {
		return nil
	}

	pagesToEvict := pageCount - pc.maxEntries

	// Sort usage keys by value in ascending order
	keys := make([]int64, 0, len(pc.usage))
	for k := range pc.usage {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return pc.usage[keys[i]] < pc.usage[keys[j]]
	})

	evictedPages := 0

	for i := 0; i < pagesToEvict; i++ {
		delete(pc.pages, keys[i])
		delete(pc.usage, keys[i])
		evictedPages += 1
	}

	log.Printf("EVICTED %d PAGES", evictedPages)

	return nil
}
