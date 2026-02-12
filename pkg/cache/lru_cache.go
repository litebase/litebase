package cache

import (
	"container/list"
	"sync"
)

var lruBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096) // 4KB default buffer size
		return &buf
	},
}

var lruCacheItemPool = sync.Pool{
	New: func() any {
		return &lruCacheItem{}
	},
}

type lruCacheItem struct {
	key   any
	value any
}

// LRUCache represents a Least Recently Used cache.
type LRUCache struct {
	capacity int
	items    map[any]*list.Element
	lruList  *list.List
	mutex    sync.Mutex
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		items:    make(map[any]*list.Element, capacity),
		lruList:  list.New(),
	}
}

func (c *LRUCache) Delete(key any) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, found := c.items[key]; found {
		c.lruList.Remove(elem)

		item := elem.Value.(*lruCacheItem)

		// Return byte slice buffer to pool
		if b, ok := item.value.([]byte); ok {
			lruBufferPool.Put(&b)
		}

		// Return item to pool
		item.key = nil
		item.value = nil
		lruCacheItemPool.Put(item)

		delete(c.items, key)
	}
}

// Get retrieves an item from the cache and marks it as recently used.
func (c *LRUCache) Get(key any) (any, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if elem, found := c.items[key]; found {
		// Move to front (most recently used)
		c.lruList.MoveToFront(elem)

		item := elem.Value.(*lruCacheItem)

		return item.value, true
	}

	return nil, false
}

// Put adds an item to the cache.
func (c *LRUCache) Put(key any, value any) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Copy byte slices using buffer from pool
	storedValue := value

	if b, ok := value.([]byte); ok {
		bufPtr := lruBufferPool.Get().(*[]byte)

		// Ensure buffer has enough capacity
		if cap(*bufPtr) < len(b) {
			*bufPtr = make([]byte, len(b))
		} else {
			*bufPtr = (*bufPtr)[:len(b)]
		}

		copy(*bufPtr, b)
		storedValue = *bufPtr
	}

	// Check if item already exists
	if elem, found := c.items[key]; found {
		// Update existing item
		item := elem.Value.(*lruCacheItem)

		// Return old buffer to pool if it was a byte slice
		if oldBuf, ok := item.value.([]byte); ok {
			lruBufferPool.Put(&oldBuf)
		}

		item.value = storedValue

		// Move to front (most recently used)
		c.lruList.MoveToFront(elem)

		return nil
	}

	// Evict least recently used if at capacity
	if c.lruList.Len() >= c.capacity {
		// Remove least recently used (back of list)
		oldest := c.lruList.Back()

		if oldest != nil {
			c.lruList.Remove(oldest)

			item := oldest.Value.(*lruCacheItem)

			// Return evicted buffer to pool
			if evictedBuf, ok := item.value.([]byte); ok {
				lruBufferPool.Put(&evictedBuf)
			}

			delete(c.items, item.key)

			// Return item to pool
			item.key = nil
			item.value = nil
			lruCacheItemPool.Put(item)
		}
	}

	// Add new item to front (most recently used)
	newItem := lruCacheItemPool.Get().(*lruCacheItem)
	newItem.key = key
	newItem.value = storedValue

	elem := c.lruList.PushFront(newItem)
	c.items[key] = elem

	return nil
}

// DeleteIf removes all cache entries where predicate(key) returns true.
// This is O(n) in the number of cache items and should be used sparingly.
func (c *LRUCache) DeleteIf(predicate func(any) bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var toRemove []*list.Element

	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*lruCacheItem)

		if predicate(item.key) {
			toRemove = append(toRemove, elem)
		}
	}

	for _, elem := range toRemove {
		c.lruList.Remove(elem)

		item := elem.Value.(*lruCacheItem)

		// Return byte slice buffer to pool
		if b, ok := item.value.([]byte); ok {
			lruBufferPool.Put(&b)
		}

		delete(c.items, item.key)

		// Return item to pool
		item.key = nil
		item.value = nil
		lruCacheItemPool.Put(item)
	}
}

// Close clears the cache and returns all buffers to the pool.
func (c *LRUCache) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Return all byte slice buffers to pool
	for elem := c.lruList.Front(); elem != nil; elem = elem.Next() {
		item := elem.Value.(*lruCacheItem)

		if b, ok := item.value.([]byte); ok {
			lruBufferPool.Put(&b)
		}

		// Return item to pool
		item.key = nil
		item.value = nil
		lruCacheItemPool.Put(item)
	}

	// Clear all items
	c.items = make(map[any]*list.Element, c.capacity)
	c.lruList.Init()
}
