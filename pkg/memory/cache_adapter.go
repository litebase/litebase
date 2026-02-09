package memory

import (
	"log/slog"

	"github.com/litebase/litebase/pkg/cache"
)

// ManagedCache wraps an LFU cache with memory manager integration
type ManagedCache struct {
	cache       *cache.LFUCache
	manager     *Manager
	sizeFunc    func(any) int64
	defaultSize int64
	owner       string
	leases      map[any]*Lease
}

// ManagedCacheConfig contains configuration for a managed cache
type ManagedCacheConfig struct {
	Capacity    int
	Manager     *Manager
	SizeFunc    func(any) int64
	DefaultSize int64
	Owner       string
}

// NewManagedCache creates a new managed cache
func NewManagedCache(cfg ManagedCacheConfig) *ManagedCache {
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

	return &ManagedCache{
		cache:       cache.NewLFUCache(cfg.Capacity),
		manager:     cfg.Manager,
		sizeFunc:    cfg.SizeFunc,
		defaultSize: cfg.DefaultSize,
		owner:       cfg.Owner,
		leases:      make(map[any]*Lease),
	}
}

// registerOwnerHandler registers a reclaim handler for this cache owner if one
// isn't already registered. It deletes cached entry and removes lease mapping
// when called.
func (mc *ManagedCache) registerOwnerHandler() {
	if mc.manager == nil || mc.owner == "" {
		return
	}

	mc.manager.RegisterReclaimHandler(mc.owner, func(l *Lease) error {
		// l.Key is expected to be the cache key used by this ManagedCache
		if l == nil {
			return nil
		}

		mc.cache.Delete(l.Key)
		delete(mc.leases, l.Key)

		return nil
	})
}

// Put adds an item to the cache
func (mc *ManagedCache) Put(key any, value any) error {
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
func (mc *ManagedCache) Get(key any) (any, bool) {
	value, found := mc.cache.Get(key)

	if found {
		// Touch the lease to update LRU
		if lease, exists := mc.leases[key]; exists {
			mc.manager.Touch(lease)
		}
	}

	return value, found
}

// Delete removes an item from the cache
func (mc *ManagedCache) Delete(key any) {
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
