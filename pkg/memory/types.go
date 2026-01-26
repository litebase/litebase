package memory

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	ErrNoMemory         = errors.New("insufficient memory available")
	ErrInvalidLease     = errors.New("invalid lease")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrManagerShutdown  = errors.New("memory manager is shutdown")
	nextLeaseID         atomic.Uint64
)

// LeaseID represents a unique identifier for a memory lease
type LeaseID string

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
	lastUsed    atomic.Int64 // Unix timestamp in nanoseconds
	OnReclaim   func() error
	Reclaimed   bool
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
