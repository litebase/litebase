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

	// Request memory lease
	lease, err := mc.manager.Request(size,
		Reclaimable(true),
		WithOwner(mc.owner),
		WithOnReclaim(func() error {
			mc.cache.Delete(key)
			delete(mc.leases, key)
			return nil
		}),
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
