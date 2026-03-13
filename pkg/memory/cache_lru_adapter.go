package memory

import (
	"log/slog"

	"github.com/litebase/litebase/pkg/cache"
)

// ManagedLRUCache wraps an LRU cache with memory manager integration
type ManagedLRUCache struct {
	cache       *cache.LRUCache
	manager     *Manager
	sizeFunc    func(any) int64
	defaultSize int64
	owner       string
	leases      map[any]*Lease
}

// ManagedLRUCacheConfig contains configuration for a managed LRU cache
type ManagedLRUCacheConfig struct {
	Capacity    int
	Manager     *Manager
	SizeFunc    func(any) int64
	DefaultSize int64
	Owner       string
}

// NewManagedLRUCache creates a new managed LRU cache
func NewManagedLRUCache(cfg ManagedLRUCacheConfig) *ManagedLRUCache {
	if cfg.DefaultSize == 0 {
		cfg.DefaultSize = 64
	}

	if cfg.SizeFunc == nil {
		cfg.SizeFunc = func(v any) int64 {
			// Try to estimate size based on type
			switch val := v.(type) {
			case []byte:
				return int64(len(val))
			case string:
				return int64(len(val))
			default:
				return cfg.DefaultSize
			}
		}
	}

	mc := &ManagedLRUCache{
		cache:       cache.NewLRUCache(cfg.Capacity),
		manager:     cfg.Manager,
		sizeFunc:    cfg.SizeFunc,
		defaultSize: cfg.DefaultSize,
		owner:       cfg.Owner,
		leases:      make(map[any]*Lease),
	}

	// Release the memory lease whenever the underlying LRU cache naturally
	// evicts an entry due to capacity pressure (or explicit Delete/Close).
	// Without this, each evicted page leaves an unreleased lease in the
	// memory manager reservoir, eventually exhausting available memory.
	mc.cache.OnEvict = func(key any) {
		if lease, ok := mc.leases[key]; ok {
			if mc.manager != nil {
				mc.manager.Release(lease) //nolint:errcheck
			}

			delete(mc.leases, key)
		}
	}

	return mc
}

// registerOwnerHandler registers a reclaim handler for this cache owner if one
// isn't already registered. It deletes cached entry and removes lease mapping
// when called.
func (mc *ManagedLRUCache) registerOwnerHandler() {
	if mc.manager == nil || mc.owner == "" {
		return
	}

	mc.manager.RegisterReclaimHandler(mc.owner, func(l *Lease) error {
		// l.Key is expected to be the cache key used by this ManagedLRUCache
		if l == nil {
			return nil
		}

		mc.cache.Delete(l.Key)
		delete(mc.leases, l.Key)

		return nil
	})
}

// Put adds an item to the cache
func (mc *ManagedLRUCache) Put(key any, value any) error {
	size := mc.sizeFunc(value)

	// Check if key exists and release old lease
	if oldLease, exists := mc.leases[key]; exists {
		err := mc.manager.Release(oldLease)

		if err != nil {
			slog.Warn("Failed to release old lease", "key", key, "error", err)
		}

		delete(mc.leases, key)
	}

	// Ensure owner-level reclaim handler is registered (register once per cache instance)
	mc.registerOwnerHandler()

	// Request memory lease; set owner so owner-level handler will be used on reclaim.
	lease, err := mc.manager.Request(size,
		Reclaimable(true),
		WithOwner(mc.owner),
	)

	if err != nil {
		return err
	}

	// Store in cache
	err = mc.cache.Put(key, value)

	if err != nil {
		// If cache put fails, release the lease
		releaseErr := mc.manager.Release(lease)

		if releaseErr != nil {
			slog.Warn("Failed to release lease after cache put failure", "key", key, "error", releaseErr)
		}

		return err
	}

	// Attach the cache key to the lease so the owner-level handler can find it
	lease.Key = key
	mc.leases[key] = lease

	return nil
}

// Get retrieves an item from the cache
func (mc *ManagedLRUCache) Get(key any) (any, bool) {
	value, found := mc.cache.Get(key)

	if found {
		// Touch the lease to update recency
		if lease, exists := mc.leases[key]; exists {
			mc.manager.Touch(lease)
		}
	}

	return value, found
}

// Delete removes an item from the cache
func (mc *ManagedLRUCache) Delete(key any) {
	// Release lease
	if lease, exists := mc.leases[key]; exists {
		err := mc.manager.Release(lease)

		if err != nil {
			slog.Warn("Failed to release lease", "key", key, "error", err)
		}

		delete(mc.leases, key)
	}

	// Remove from cache
	mc.cache.Delete(key)
}
