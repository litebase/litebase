package database

import "github.com/litebase/litebase/pkg/memory"

// SetWALCacheForTest replaces the internal cache of a WAL instance. This is
// intended for tests only to simulate low-capacity caches and eviction.
func SetWALCacheForTest(w *DatabaseWAL, c *memory.ManagedLRUCache) {
    w.mutex.Lock()
    defer w.mutex.Unlock()
    w.cache = c
}
