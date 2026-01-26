package memory

import (
	"sort"
	"time"
)

// EvictionPolicy defines how leases are selected for eviction
type EvictionPolicy interface {
	SelectForEviction(leases []*Lease, needed int64) []*Lease
}

// LRUEvictionPolicy evicts least recently used leases first
type LRUEvictionPolicy struct {
	priorityWeights map[Priority]float64
}

// NewLRUEvictionPolicy creates a new LRU eviction policy
func NewLRUEvictionPolicy() *LRUEvictionPolicy {
	return &LRUEvictionPolicy{
		priorityWeights: map[Priority]float64{
			PriorityLow:      1.0,
			PriorityNormal:   2.0,
			PriorityHigh:     4.0,
			PriorityCritical: 8.0,
		},
	}
}

// SelectForEviction selects leases to evict to free the needed amount of memory
func (p *LRUEvictionPolicy) SelectForEviction(leases []*Lease, needed int64) []*Lease {
	// Filter to only reclaimable leases
	reclaimable := make([]*Lease, 0, len(leases))

	for _, lease := range leases {
		if lease.Reclaimable {
			reclaimable = append(reclaimable, lease)
		}
	}

	if len(reclaimable) == 0 {
		return nil
	}

	// Score each lease based on LRU and priority
	now := time.Now().UTC()

	type scoredLease struct {
		lease *Lease
		score float64
	}

	scored := make([]scoredLease, len(reclaimable))

	for i, lease := range reclaimable {
		// Calculate age in seconds since last use
		age := now.Sub(lease.GetLastUsed()).Seconds()

		// Higher score = more likely to evict
		// Age / priority weight (lower priority = higher score)
		priorityWeight := p.priorityWeights[lease.Priority]
		score := age / priorityWeight

		scored[i] = scoredLease{lease: lease, score: score}
	}

	// Sort by score descending (highest score first = oldest + lowest priority)
	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	// Select leases until we have enough memory
	selected := make([]*Lease, 0)
	var freed int64

	for _, sl := range scored {
		selected = append(selected, sl.lease)
		freed += sl.lease.Size

		if freed >= needed {
			break
		}
	}

	return selected
}

// SizeBasedEvictionPolicy evicts largest leases first
type SizeBasedEvictionPolicy struct{}

// NewSizeBasedEvictionPolicy creates a new size-based eviction policy
func NewSizeBasedEvictionPolicy() *SizeBasedEvictionPolicy {
	return &SizeBasedEvictionPolicy{}
}

// SelectForEviction selects the largest reclaimable leases
func (p *SizeBasedEvictionPolicy) SelectForEviction(leases []*Lease, needed int64) []*Lease {
	// Filter to only reclaimable leases
	reclaimable := make([]*Lease, 0, len(leases))

	for _, lease := range leases {
		if lease.Reclaimable {
			reclaimable = append(reclaimable, lease)
		}
	}

	if len(reclaimable) == 0 {
		return nil
	}

	// Sort by size descending (largest first)
	sort.Slice(reclaimable, func(i, j int) bool {
		return reclaimable[i].Size > reclaimable[j].Size
	})

	// Select leases until we have enough memory
	selected := make([]*Lease, 0)
	var freed int64

	for _, lease := range reclaimable {
		selected = append(selected, lease)
		freed += lease.Size

		if freed >= needed {
			break
		}
	}

	return selected
}
