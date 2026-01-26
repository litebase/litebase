package memory_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/memory"
)

func TestLRUEvictionPolicy(t *testing.T) {
	policy := memory.NewLRUEvictionPolicy()
	now := time.Now().UTC()

	t.Run("SelectOldest", func(t *testing.T) {
		newLease := &memory.Lease{ID: "new", Size: 100, Reclaimable: true, Priority: memory.PriorityNormal}
		oldLease := &memory.Lease{ID: "old", Size: 100, Reclaimable: true, Priority: memory.PriorityNormal}

		// Set last used times using the Touch method indirectly or by setting the atomic value
		newLease.SetLastUsed(now)
		oldLease.SetLastUsed(now.Add(-1 * time.Hour))

		leases := []*memory.Lease{newLease, oldLease}

		selected := policy.SelectForEviction(leases, 100)

		if len(selected) != 1 {
			t.Fatalf("Expected 1 lease, got %d", len(selected))
		}

		if selected[0].ID != "old" {
			t.Errorf("Expected oldest lease, got %s", selected[0].ID)
		}
	})

	t.Run("IgnoreNonReclaimable", func(t *testing.T) {
		reclaimableLease := &memory.Lease{ID: "reclaimable", Size: 100, Reclaimable: true, Priority: memory.PriorityNormal}
		nonReclaimableLease := &memory.Lease{ID: "non-reclaimable", Size: 100, Reclaimable: false, Priority: memory.PriorityNormal}

		reclaimableLease.SetLastUsed(now)
		nonReclaimableLease.SetLastUsed(now.Add(-1 * time.Hour))

		leases := []*memory.Lease{reclaimableLease, nonReclaimableLease}

		selected := policy.SelectForEviction(leases, 100)

		if selected[0].ID != "reclaimable" {
			t.Errorf("Expected reclaimable lease, got %s", selected[0].ID)
		}
	})
}

func TestSizeBasedEvictionPolicy(t *testing.T) {
	policy := memory.NewSizeBasedEvictionPolicy()
	now := time.Now().UTC()

	t.Run("SelectLargest", func(t *testing.T) {
		smallLease := &memory.Lease{ID: "small", Size: 100, Reclaimable: true}
		largeLease := &memory.Lease{ID: "large", Size: 500, Reclaimable: true}

		smallLease.SetLastUsed(now)
		largeLease.SetLastUsed(now)

		leases := []*memory.Lease{smallLease, largeLease}

		selected := policy.SelectForEviction(leases, 250)

		if selected[0].ID != "large" {
			t.Errorf("Expected largest lease, got %s", selected[0].ID)
		}
	})
}
