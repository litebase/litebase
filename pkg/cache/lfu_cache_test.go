package cache_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/cache"
)

func TestLFUCacheDelete(t *testing.T) {
	c := cache.NewLFUCache(2)

	// Add items to the cache
	c.Put("key1", []byte("value1"))
	c.Put("key2", []byte("value2"))

	// Delete an item from the cache
	c.Delete("key2")

	_, found := c.Get("key2")

	if found {
		t.Fatal("Expected key2 to be deleted")
	}

	// Verify that key1 is still in the cache
	_, found = c.Get("key1")

	if !found {
		t.Fatal("Expected key1 to still be in the cache")
	}
}

func TestLFUCache_PutAndGet(t *testing.T) {
	c := cache.NewLFUCache(2)

	// Add items to the cache
	c.Put("key1", []byte("value1"))
	c.Put("key2", []byte("value2"))

	// Retrieve items and verify
	value, found := c.Get("key1")

	if !found || string(value.([]byte)) != "value1" {
		t.Fatalf("Expected to find key1 with value 'value1', got %v", value)
	}

	value, found = c.Get("key2")

	if !found || string(value.([]byte)) != "value2" {
		t.Fatalf("Expected to find key2 with value 'value2', got %v", value)
	}
}

func TestLFUCache_EvictLeastFrequentlyUsed(t *testing.T) {
	c := cache.NewLFUCache(2)

	// Add items to the cache
	c.Put("key1", []byte("value1"))
	c.Put("key2", []byte("value2"))

	// Access key1 to increase its frequency
	c.Get("key1")

	// Add a new item, causing eviction
	c.Put("key3", []byte("value3"))

	// Verify that key2 was evicted
	_, found := c.Get("key2")

	if found {
		t.Fatal("Expected key2 to be evicted")
	}

	// Verify that key1 and key3 are still in the cache
	_, found = c.Get("key1")

	if !found {
		t.Fatal("Expected key1 to still be in the cache")
	}

	_, found = c.Get("key3")

	if !found {
		t.Fatal("Expected key3 to still be in the cache")
	}
}

func TestLFUCache_UpdateExistingKey(t *testing.T) {
	c := cache.NewLFUCache(2)

	// Add an item to the cache
	c.Put("key1", []byte("value1"))

	// Update the value of the existing key
	c.Put("key1", []byte("value1_updated"))

	// Retrieve the updated value
	value, found := c.Get("key1")

	if !found || string(value.([]byte)) != "value1_updated" {
		t.Fatalf("Expected to find key1 with value 'value1_updated', got %v", value)
	}
}

func TestLFUCache_CapacityZero(t *testing.T) {
	c := cache.NewLFUCache(0)

	// Attempt to add an item to the cache
	err := c.Put("key1", []byte("value1"))

	if err != nil {
		t.Fatalf("Expected no error when adding item to cache with capacity 0, got: %v", err)
	}

	// Verify that the item was not added
	_, found := c.Get("key1")

	if !found {
		t.Fatal("Expected key1 to not be added to the cache with capacity 0")
	}
}

func TestLFUCache_FrequencyUpdate(t *testing.T) {
	c := cache.NewLFUCache(2)

	// Add items to the cache
	c.Put("key1", []byte("value1"))
	c.Put("key2", []byte("value2"))

	// Access key1 multiple times to increase its frequency
	c.Get("key1")
	c.Get("key1")

	// Add a new item, causing eviction
	c.Put("key3", []byte("value3"))

	// Verify that key2 was evicted (least frequently used)
	_, found := c.Get("key2")

	if found {
		t.Fatal("Expected key2 to be evicted")
	}

	// Verify that key1 and key3 are still in the cache
	_, found = c.Get("key1")

	if !found {
		t.Fatal("Expected key1 to still be in the cache")
	}

	_, found = c.Get("key3")

	if !found {
		t.Fatal("Expected key3 to still be in the cache")
	}
}
