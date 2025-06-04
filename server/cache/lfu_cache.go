package cache

import (
	"container/heap"
)

type CacheItem struct {
	key       string
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

func (c *LFUCache) Delete(key string) {
	if item, found := c.items[key]; found {
		heap.Remove(&c.pq, item.index)
		delete(c.items, key)
	}
}

// Get retrieves an item from the cache.
func (c *LFUCache) Get(key string) (any, bool) {
	if item, found := c.items[key]; found {
		item.frequency++
		heap.Fix(&c.pq, item.index)
		return item.value, true
	}

	return nil, false
}

// Put adds an item to the cache.
func (c *LFUCache) Put(key string, value any) error {
	item, found := c.items[key]

	if found {
		item.value = value
		item.frequency++
		heap.Fix(&c.pq, item.index)

		return nil
	}

	if len(c.items) >= c.capacity {
		// Remove the least frequently used item.
		if lfuItem, ok := heap.Pop(&c.pq).(*CacheItem); ok {
			delete(c.items, lfuItem.key)
		}
	}

	// Add the new item.
	newItem := &CacheItem{
		key:       key,
		value:     value,
		frequency: 1,
	}

	heap.Push(&c.pq, newItem)

	c.items[key] = newItem

	return nil
}
