package cache

import (
	"container/heap"
)

type CacheItem struct {
	key       string
	value     []byte
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
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*CacheItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // For safety
	*pq = old[0 : n-1]
	return item
}

// LFUCache represents a Least Frequently Used cache.
type LFUCache struct {
	capacity int
	items    map[string]*CacheItem
	pq       PriorityQueue
}

func NewLFUCache(capacity int) *LFUCache {
	return &LFUCache{
		capacity: capacity,
		items:    make(map[string]*CacheItem),
		pq:       make(PriorityQueue, 0, capacity),
	}
}

// Get retrieves an item from the cache.
func (c *LFUCache) Get(key string) ([]byte, bool) {
	if item, found := c.items[key]; found {
		item.frequency++
		heap.Fix(&c.pq, item.index)
		return item.value, true
	}

	return nil, false
}

// Put adds an item to the cache.
func (c *LFUCache) Put(key string, value []byte) {
	if c.capacity == 0 {
		return
	}

	if item, found := c.items[key]; found {
		item.value = value
		item.frequency++
		heap.Fix(&c.pq, item.index)

		return
	}

	if len(c.items) >= c.capacity {
		// Remove the least frequently used item.
		lfuItem := heap.Pop(&c.pq).(*CacheItem)
		delete(c.items, lfuItem.key)
	}

	// Add the new item.
	newItem := &CacheItem{
		key:       key,
		value:     value,
		frequency: 1,
	}

	heap.Push(&c.pq, newItem)

	c.items[key] = newItem
}
