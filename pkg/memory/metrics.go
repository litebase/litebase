package memory

import (
	"sync"
	"sync/atomic"
)

// Metrics tracks memory usage statistics
type Metrics struct {
	totalReserved   atomic.Int64
	totalReleased   atomic.Int64
	leaseCount      atomic.Int64
	evictionCount   atomic.Int64
	reclaimFailures atomic.Int64
	byComponent     map[string]*atomic.Int64
	componentMutex  sync.RWMutex
}

// NewMetrics creates a new metrics tracker
func NewMetrics() *Metrics {
	return &Metrics{
		byComponent: make(map[string]*atomic.Int64),
	}
}

// RecordAllocation records a memory allocation
func (m *Metrics) RecordAllocation(size int64, component string) {
	m.totalReserved.Add(size)
	m.leaseCount.Add(1)

	if component != "" {
		m.componentMutex.Lock()

		if _, exists := m.byComponent[component]; !exists {
			m.byComponent[component] = &atomic.Int64{}
		}

		counter := m.byComponent[component]

		m.componentMutex.Unlock()

		counter.Add(size)
	}
}

// RecordRelease records a memory release
func (m *Metrics) RecordRelease(size int64, component string) {
	m.totalReleased.Add(size)

	if component != "" {
		m.componentMutex.RLock()

		if counter, exists := m.byComponent[component]; exists {
			counter.Add(-size)
		}

		m.componentMutex.RUnlock()
	}
}

// RecordEviction records an eviction event
func (m *Metrics) RecordEviction(size int64) {
	m.evictionCount.Add(1)
	m.totalReleased.Add(size)
}

// RecordReclaimFailure records a failed reclaim attempt
func (m *Metrics) RecordReclaimFailure() {
	m.reclaimFailures.Add(1)
}

// GetTotalReserved returns total memory ever reserved
func (m *Metrics) GetTotalReserved() int64 {
	return m.totalReserved.Load()
}

// GetTotalReleased returns total memory ever released
func (m *Metrics) GetTotalReleased() int64 {
	return m.totalReleased.Load()
}

// GetLeaseCount returns current number of active leases
func (m *Metrics) GetLeaseCount() int64 {
	return m.leaseCount.Load()
}

// GetEvictionCount returns number of evictions
func (m *Metrics) GetEvictionCount() int64 {
	return m.evictionCount.Load()
}

// GetReclaimFailures returns number of failed reclaim attempts
func (m *Metrics) GetReclaimFailures() int64 {
	return m.reclaimFailures.Load()
}

// GetMemoryByComponent returns memory usage by component
func (m *Metrics) GetMemoryByComponent() map[string]int64 {
	m.componentMutex.RLock()
	defer m.componentMutex.RUnlock()

	result := make(map[string]int64, len(m.byComponent))

	for component, counter := range m.byComponent {
		result[component] = counter.Load()
	}

	return result
}

// Snapshot returns a point-in-time snapshot of metrics
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		TotalReserved:   m.totalReserved.Load(),
		TotalReleased:   m.totalReleased.Load(),
		LeaseCount:      m.leaseCount.Load(),
		EvictionCount:   m.evictionCount.Load(),
		ReclaimFailures: m.reclaimFailures.Load(),
		ByComponent:     m.GetMemoryByComponent(),
	}
}

// MetricsSnapshot is a point-in-time snapshot
type MetricsSnapshot struct {
	TotalReserved   int64
	TotalReleased   int64
	LeaseCount      int64
	EvictionCount   int64
	ReclaimFailures int64
	ByComponent     map[string]int64
}
