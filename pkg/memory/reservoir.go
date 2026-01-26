package memory

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Reservoir manages a fixed pool of memory
type Reservoir struct {
	capacity  int64
	reserved  atomic.Int64
	threshold float64
	leases    map[LeaseID]*Lease
	mutex     sync.RWMutex
}

// NewReservoir creates a new memory reservoir
func NewReservoir(capacity int64, threshold float64) *Reservoir {
	return &Reservoir{
		capacity:  capacity,
		threshold: threshold,
		leases:    make(map[LeaseID]*Lease),
	}
}

// Reserve attempts to reserve memory
func (r *Reservoir) Reserve(size int64) (int64, error) {
	reserved := r.reserved.Add(size)

	if reserved > r.capacity {
		r.reserved.Add(-size)

		return 0, ErrNoMemory
	}

	return reserved, nil
}

// Release releases reserved memory
func (r *Reservoir) Release(size int64) {
	r.reserved.Add(-size)
}

// GetReserved returns the currently reserved amount
func (r *Reservoir) GetReserved() int64 {
	return r.reserved.Load()
}

// GetAvailable returns available memory
func (r *Reservoir) GetAvailable() int64 {
	return r.capacity - r.reserved.Load()
}

// IsUnderPressure checks if memory usage exceeds threshold
func (r *Reservoir) IsUnderPressure() bool {
	reserved := float64(r.reserved.Load())
	capacity := float64(r.capacity)

	return (reserved / capacity) >= r.threshold
}

// AddLease registers a lease
func (r *Reservoir) AddLease(lease *Lease) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.leases[lease.ID] = lease
}

// RemoveLease unregisters a lease
func (r *Reservoir) RemoveLease(id LeaseID) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.leases, id)
}

// GetLease retrieves a lease by ID
func (r *Reservoir) GetLease(id LeaseID) (*Lease, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	lease, ok := r.leases[id]

	return lease, ok
}

// GetAllLeases returns all registered leases
func (r *Reservoir) GetAllLeases() []*Lease {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	leases := make([]*Lease, 0, len(r.leases))

	for _, lease := range r.leases {
		leases = append(leases, lease)
	}

	return leases
}

// Stats returns reservoir statistics
func (r *Reservoir) Stats() ReservoirStats {
	reserved := r.reserved.Load()

	return ReservoirStats{
		Capacity:           r.capacity,
		Reserved:           reserved,
		Available:          r.capacity - reserved,
		UnderPressure:      r.IsUnderPressure(),
		UtilizationPercent: (float64(reserved) / float64(r.capacity)) * 100,
	}
}

// ReservoirStats contains reservoir statistics
type ReservoirStats struct {
	Capacity           int64
	Reserved           int64
	Available          int64
	UnderPressure      bool
	UtilizationPercent float64
}

// String returns a string representation of stats
func (s ReservoirStats) String() string {
	return fmt.Sprintf("Capacity=%d Reserved=%d Available=%d Utilization=%.2f%% Pressure=%v",
		s.Capacity, s.Reserved, s.Available, s.UtilizationPercent, s.UnderPressure)
}
