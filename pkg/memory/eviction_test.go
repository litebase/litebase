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
		newLease := &memory.Lease{ID: 1, Size: 100, Reclaimable: true, Priority: memory.PriorityNormal}
		oldLease := &memory.Lease{ID: 2, Size: 100, Reclaimable: true, Priority: memory.PriorityNormal}

		// Set last used times using the Touch method indirectly or by setting the atomic value
		newLease.SetLastUsed(now)
		oldLease.SetLastUsed(now.Add(-1 * time.Hour))

		leases := []*memory.Lease{newLease, oldLease}

		selected := policy.SelectForEviction(leases, 100)

		if len(selected) != 1 {
			t.Fatalf("Expected 1 lease, got %d", len(selected))
		}

		if selected[0].ID != 2 {
			t.Errorf("Expected oldest lease, got %d", selected[0].ID)
		}
	})

	t.Run("IgnoreNonReclaimable", func(t *testing.T) {
		reclaimableLease := &memory.Lease{ID: 3, Size: 100, Reclaimable: true, Priority: memory.PriorityNormal}
		nonReclaimableLease := &memory.Lease{ID: 4, Size: 100, Reclaimable: false, Priority: memory.PriorityNormal}

		reclaimableLease.SetLastUsed(now)
		nonReclaimableLease.SetLastUsed(now.Add(-1 * time.Hour))

		leases := []*memory.Lease{reclaimableLease, nonReclaimableLease}

		selected := policy.SelectForEviction(leases, 100)

		if selected[0].ID != 3 {
			t.Errorf("Expected reclaimable lease, got %d", selected[0].ID)
		}
	})
}

func TestSizeBasedEvictionPolicy(t *testing.T) {
	policy := memory.NewSizeBasedEvictionPolicy()
	now := time.Now().UTC()

	t.Run("SelectLargest", func(t *testing.T) {
		smallLease := &memory.Lease{ID: 5, Size: 100, Reclaimable: true}
		largeLease := &memory.Lease{ID: 6, Size: 500, Reclaimable: true}

		smallLease.SetLastUsed(now)
		largeLease.SetLastUsed(now)

		leases := []*memory.Lease{smallLease, largeLease}

		selected := policy.SelectForEviction(leases, 250)

		if selected[0].ID != 6 {
			t.Errorf("Expected largest lease, got %d", selected[0].ID)
		}
	})
}
