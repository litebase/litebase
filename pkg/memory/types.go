package memory

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNoMemory        = errors.New("insufficient memory available")
	ErrInvalidLease    = errors.New("invalid lease")
	ErrLeaseNotFound   = errors.New("lease not found")
	ErrManagerShutdown = errors.New("memory manager is shutdown")
	nextLeaseID        atomic.Uint64
)

// LeaseID represents a unique identifier for a memory lease
// Use a numeric ID to avoid string allocations for high-frequency IDs.
type LeaseID uint64

// Priority defines the importance of a memory lease for eviction decisions
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// Lease represents a memory allocation
type Lease struct {
	ID          LeaseID
	Size        int64
	Reclaimable bool
	Priority    Priority
	Owner       string
	// Key is optional metadata associated with the lease (e.g., cache key).
	// It may be used by owner-level reclaim handlers to identify the resource to delete.
	Key       any
	lastUsed  atomic.Int64 // Unix timestamp in nanoseconds
	OnReclaim func() error
	Reclaimed bool
	// Slab is the actual memory backing allocated by the manager.
	// When non-nil, this slab is owned by the manager and will be freed on Release/eviction.
	Slab      []byte
}

var leasePool = sync.Pool{
	New: func() any {
		return &Lease{}
	},
}

// AcquireLease gets a Lease from the pool (zeroed) for reuse.
func AcquireLease() *Lease {
	l := leasePool.Get().(*Lease)

	// Reset fields to zero state
	l.ID = 0
	l.Size = 0
	l.Reclaimable = false
	l.Priority = 0
	l.Owner = ""
	l.Key = nil
	l.lastUsed.Store(0)
	l.OnReclaim = nil
	l.Reclaimed = false

	return l
}

// ReleaseLease resets the lease and returns it to the pool.
func ReleaseLease(l *Lease) {
	if l == nil {
		return
	}

	l.ID = 0
	l.Size = 0
	l.Reclaimable = false
	l.Priority = 0
	l.Owner = ""
	l.Key = nil
	l.lastUsed.Store(0)
	l.OnReclaim = nil
	l.Reclaimed = false
	l.Slab = nil // Release slab reference for GC

	leasePool.Put(l)
}

// GetLastUsed returns the last used time as a time.Time
func (l *Lease) GetLastUsed() time.Time {
	nanos := l.lastUsed.Load()

	if nanos == 0 {
		return time.Time{}
	}

	return time.Unix(0, nanos)
}

// SetLastUsed sets the last used time (for testing purposes)
func (l *Lease) SetLastUsed(t time.Time) {
	l.lastUsed.Store(t.UnixNano())
}

// LeaseOption is a functional option for configuring a Lease
type LeaseOption func(*Lease)

// Reclaimable sets whether the lease can be reclaimed
func Reclaimable(r bool) LeaseOption {
	return func(l *Lease) {
		l.Reclaimable = r
	}
}

// WithPriority sets the priority of the lease
func WithPriority(p Priority) LeaseOption {
	return func(l *Lease) {
		l.Priority = p
	}
}

// WithOwner sets the owner/component name of the lease
func WithOwner(owner string) LeaseOption {
	return func(l *Lease) {
		l.Owner = owner
	}
}

// WithOnReclaim sets the callback function when lease is reclaimed
func WithOnReclaim(fn func() error) LeaseOption {
	return func(l *Lease) {
		l.OnReclaim = fn
	}
}
