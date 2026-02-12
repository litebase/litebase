package memory

import (
	"fmt"
	"sync"
	"time"
)

// Manager coordinates memory allocation across components
type Manager struct {
	reservoir      *Reservoir
	metrics        *Metrics
	evictionPolicy EvictionPolicy
	mutex          sync.Mutex
	shutdown       bool
	// reclaimHandlers stores per-owner reclaim callbacks to avoid per-lease closures.
	reclaimHandlers map[string]func(*Lease) error
}

// Config contains manager configuration
type Config struct {
	Capacity       int64
	Threshold      float64
	EvictionPolicy EvictionPolicy
}

// NewManager creates a new memory manager
func NewManager(cfg Config) (*Manager, error) {
	if cfg.Capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}

	if cfg.Threshold <= 0 || cfg.Threshold > 1 {
		cfg.Threshold = 0.9
	}

	if cfg.EvictionPolicy == nil {
		cfg.EvictionPolicy = NewLRUEvictionPolicy()
	}

	return &Manager{
		reservoir:       NewReservoir(cfg.Capacity, cfg.Threshold),
		metrics:         NewMetrics(),
		evictionPolicy:  cfg.EvictionPolicy,
		reclaimHandlers: make(map[string]func(*Lease) error),
	}, nil
}

// RegisterReclaimHandler registers a reclaim handler for a given owner.
// The handler will be invoked for leases owned by that owner when the manager
// needs to reclaim or evict leases. This avoids allocating a closure per-lease.
func (m *Manager) RegisterReclaimHandler(owner string, handler func(*Lease) error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if handler == nil {
		delete(m.reclaimHandlers, owner)
		return
	}

	m.reclaimHandlers[owner] = handler
}

// Request requests a memory lease without allocating a slab.
// Deprecated: prefer RequestWithSlab for true memory allocation.
func (m *Manager) Request(size int64, opts ...LeaseOption) (*Lease, error) {
	return m.requestInternal(size, false, opts...)
}

// RequestWithSlab requests a memory lease and allocates a slab of the requested size.
// The slab is owned by the manager and will be freed when the lease is released or evicted.
func (m *Manager) RequestWithSlab(size int64, opts ...LeaseOption) (*Lease, error) {
	return m.requestInternal(size, true, opts...)
}

// requestInternal is the internal implementation of Request and RequestWithSlab.
func (m *Manager) requestInternal(size int64, allocateSlab bool, opts ...LeaseOption) (*Lease, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.shutdown {
		return nil, ErrManagerShutdown
	}

	// Try to reserve memory
	_, err := m.reservoir.Reserve(size)

	if err == ErrNoMemory {
		// Try eviction
		err = m.evict(size)

		if err != nil {
			return nil, err
		}

		// Try again after eviction
		_, err = m.reservoir.Reserve(size)

		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// Create lease ID as numeric to avoid string allocations
	id := LeaseID(nextLeaseID.Add(1))

	lease := AcquireLease()
	lease.ID = id
	lease.Size = size
	lease.Reclaimable = true
	lease.Priority = PriorityNormal
	lease.lastUsed.Store(time.Now().UTC().UnixNano())

	// Apply options
	for _, opt := range opts {
		opt(lease)
	}

	// Allocate slab if requested
	if allocateSlab {
		lease.Slab = make([]byte, size)
	}

	// Register lease
	m.reservoir.AddLease(lease)
	m.metrics.RecordAllocation(size, lease.Owner)

	return lease, nil
}

// Release releases a memory lease and frees the associated slab if present.
func (m *Manager) Release(lease *Lease) error {
	if lease == nil {
		return ErrInvalidLease
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.shutdown {
		return ErrManagerShutdown
	}

	// Remove from reservoir
	m.reservoir.RemoveLease(lease.ID)
	m.reservoir.Release(lease.Size)
	m.metrics.RecordRelease(lease.Size, lease.Owner)

	// Free slab before returning lease to pool (allows GC to collect)
	lease.Slab = nil

	// Return lease to pool
	ReleaseLease(lease)

	return nil
}

// Touch updates the last used time of a lease
func (m *Manager) Touch(lease *Lease) {
	if lease != nil {
		lease.lastUsed.Store(time.Now().UTC().UnixNano())
	}
}

// CanAllocate checks if allocation is possible without eviction
func (m *Manager) CanAllocate(size int64) bool {
	return m.reservoir.GetAvailable() >= size
}

// GetStats returns current memory statistics
func (m *Manager) GetStats() ReservoirStats {
	return m.reservoir.Stats()
}

// GetMetrics returns metrics tracker
func (m *Manager) GetMetrics() *Metrics {
	return m.metrics
}

// Shutdown releases all leases and shuts down the manager
func (m *Manager) Shutdown() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.shutdown {
		return nil
	}

	m.shutdown = true

	// Release all leases
	leases := m.reservoir.GetAllLeases()

	for _, lease := range leases {
		if lease.OnReclaim != nil {
			err := lease.OnReclaim()

			if err != nil {
				m.metrics.RecordReclaimFailure()
			}
		} else if handler, ok := m.reclaimHandlers[lease.Owner]; ok && handler != nil {
			if err := handler(lease); err != nil {
				m.metrics.RecordReclaimFailure()
			}
		}

		m.reservoir.RemoveLease(lease.ID)
		m.reservoir.Release(lease.Size)

		// Return lease to pool
		ReleaseLease(lease)
	}

	return nil
}

// evict attempts to free memory by evicting leases
func (m *Manager) evict(needed int64) error {
	leases := m.reservoir.GetAllLeases()

	toEvict := m.evictionPolicy.SelectForEviction(leases, needed)

	if len(toEvict) == 0 {
		return ErrNoMemory
	}

	var freed int64

	for _, lease := range toEvict {
		// Call reclaim callback if set, otherwise call owner-level handler if registered
		if lease.OnReclaim != nil {
			if err := lease.OnReclaim(); err != nil {
				m.metrics.RecordReclaimFailure()
				continue
			}
		} else if handler, ok := m.reclaimHandlers[lease.Owner]; ok && handler != nil {
			if err := handler(lease); err != nil {
				m.metrics.RecordReclaimFailure()
				continue
			}
		}

		// Mark as reclaimed
		lease.Reclaimed = true

		// Remove and release
		m.reservoir.RemoveLease(lease.ID)
		m.reservoir.Release(lease.Size)
		m.metrics.RecordEviction(lease.Size)
		m.metrics.RecordRelease(lease.Size, lease.Owner)

		// Return lease to pool
		ReleaseLease(lease)

		freed += lease.Size
	}

	if freed < needed {
		return ErrNoMemory
	}

	return nil
}
