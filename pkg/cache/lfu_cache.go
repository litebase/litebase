package cache

import (
	"container/heap"
	"sync"
)

var lfuBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096) // 4KB default buffer size
		return &buf
	},
}

var lfuCacheItemPool = sync.Pool{
	New: func() any {
		return &CacheItem{}
	},
}

type CacheItem struct {
	key       any
	value     any
	frequency int
	index     int
}

// PriorityQueue implements a priority queue for CacheItems based on frequency.
type PriorityQueue []*CacheItem

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].frequency < pq[j].frequency
}

func (pq PriorityQueue) Swap(i, j int) {
	if len(pq) == 0 || i >= len(pq) || j >= len(pq) {
		return
	}

	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*CacheItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	item.index = -1 // For safety
	*pq = old[0 : n-1]

	return item
}

// LFUCache represents a Least Frequently Used cache.
type LFUCache struct {
	capacity int
	items    map[any]*CacheItem
	mutex    sync.Mutex
	pq       PriorityQueue
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		capacity: capacity,
		items:    make(map[any]*CacheItem),
		pq:       make(PriorityQueue, 0, capacity),
	}
}

func (c *LFUCache) Delete(key any) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, found := c.items[key]; found {
		// Return byte slice buffer to pool
		if b, ok := item.value.([]byte); ok {
			lfuBufferPool.Put(&b)
		}

		heap.Remove(&c.pq, item.index)
		delete(c.items, key)
	}
}

// Get retrieves an item from the cache.
func (c *LFUCache) Get(key any) (any, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, found := c.items[key]; found {
		item.frequency++
		heap.Fix(&c.pq, item.index)
		return item.value, true
	}

	return nil, false
}

// Put adds an item to the cache.
// If an existing item is evicted to make room, its key is returned as evictedKey
// with evicted=true so callers can release associated resources.
func (c *LFUCache) Put(key any, value any) (evicted bool, evictedKey any, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Copy byte slices using buffer from pool
	storedValue := value

	if b, ok := value.([]byte); ok {
		bufPtr := lfuBufferPool.Get().(*[]byte)

		// Ensure buffer has enough capacity
		if cap(*bufPtr) < len(b) {
			*bufPtr = make([]byte, len(b))
		} else {
			*bufPtr = (*bufPtr)[:len(b)]
		}

		copy(*bufPtr, b)
		storedValue = *bufPtr
	}

	item, found := c.items[key]

	if found {
		// Return old buffer to pool if it was a byte slice
		if oldBuf, ok := item.value.([]byte); ok {
			lfuBufferPool.Put(&oldBuf)
		}

		item.value = storedValue
		item.frequency++
		heap.Fix(&c.pq, item.index)

		return false, nil, nil
	}

	if len(c.items) >= c.capacity {
		// Remove the least frequently used item.
		if lfuItem, ok := heap.Pop(&c.pq).(*CacheItem); ok {
			// Return evicted buffer to pool
			if evictedBuf, ok := lfuItem.value.([]byte); ok {
				lfuBufferPool.Put(&evictedBuf)
			}

			deleted := lfuItem.key
			delete(c.items, lfuItem.key)

			// Return CacheItem to pool
			lfuItem.key = nil
			lfuItem.value = nil
			lfuItem.frequency = 0
			lfuCacheItemPool.Put(lfuItem)

			// Insert new item then return the evicted key.
			newItem := lfuCacheItemPool.Get().(*CacheItem)
			newItem.key = key
			newItem.value = storedValue
			newItem.frequency = 1
			heap.Push(&c.pq, newItem)
			c.items[key] = newItem

			return true, deleted, nil
		}
	}

	// Get CacheItem from pool
	newItem := lfuCacheItemPool.Get().(*CacheItem)
	newItem.key = key
	newItem.value = storedValue
	newItem.frequency = 1

	heap.Push(&c.pq, newItem)

	c.items[key] = newItem

	return false, nil, nil
}

// DeleteIf removes all cache entries where predicate(key) returns true.
// This is O(n) in the number of cache items and should be used sparingly.
func (c *LFUCache) DeleteIf(predicate func(any) bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for k, item := range c.items {
		if predicate(k) {
			// Return byte slice buffer to pool
			if b, ok := item.value.([]byte); ok {
				lfuBufferPool.Put(&b)
			}
			// remove from heap and map
			heap.Remove(&c.pq, item.index)
			delete(c.items, k)
		}
	}
}

// Close clears the cache and returns all buffers to the pool.
func (c *LFUCache) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Return all byte slice buffers to pool
	for _, item := range c.items {
		if b, ok := item.value.([]byte); ok {
			lfuBufferPool.Put(&b)
		}
	}

	// Clear all items
	c.items = make(map[any]*CacheItem)
	c.pq = make(PriorityQueue, 0, c.capacity)
}
