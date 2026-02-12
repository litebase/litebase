package cache_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/cache"
)

func TestLRUCacheDelete(t *testing.T) {
	c := cache.NewLRUCache(2)

	// Add items to the cache
	err := c.Put("key1", []byte("value1"))

	if err != nil {
		t.Fatalf("Expected no error when adding item to cache, got: %v", err)
	}

	err = c.Put("key2", []byte("value2"))

	if err != nil {
		t.Fatalf("Expected no error when adding item to cache, got: %v", err)
	}

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

func TestLRUCache_PutAndGet(t *testing.T) {
	c := cache.NewLRUCache(2)

	// Add items to the cache
	err := c.Put("key1", []byte("value1"))

	if err != nil {
		t.Fatalf("Expected no error when adding item to cache, got: %v", err)
	}

	err = c.Put("key2", []byte("value2"))

	if err != nil {
		t.Fatalf("Expected no error when adding item to cache, got: %v", err)
	}

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

func TestLRUCache_EvictLeastRecentlyUsed(t *testing.T) {
	c := cache.NewLRUCache(2)

	// Add items to the cache
	err := c.Put("key1", []byte("value1"))

	if err != nil {
		t.Fatalf("Expected no error when adding key1, got: %v", err)
	}

	err = c.Put("key2", []byte("value2"))

	if err != nil {
		t.Fatalf("Expected no error when adding key2, got: %v", err)
	}

	// Access key1 to make it more recently used
	c.Get("key1")

	// Add a third item, which should evict key2 (least recently used)
	err = c.Put("key3", []byte("value3"))

	if err != nil {
		t.Fatalf("Expected no error when adding key3, got: %v", err)
	}

	// key2 should be evicted
	_, found := c.Get("key2")

	if found {
		t.Fatal("Expected key2 to be evicted")
	}

	// key1 and key3 should still be in the cache
	_, found = c.Get("key1")

	if !found {
		t.Fatal("Expected key1 to still be in the cache")
	}

	_, found = c.Get("key3")

	if !found {
		t.Fatal("Expected key3 to be in the cache")
	}
}
