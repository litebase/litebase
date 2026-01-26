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
		leases := []*memory.Lease{
			{ID: "new", Size: 100, Reclaimable: true, Priority: memory.PriorityNormal, LastUsed: now},
			{ID: "old", Size: 100, Reclaimable: true, Priority: memory.PriorityNormal, LastUsed: now.Add(-1 * time.Hour)},
		}
		
		selected := policy.SelectForEviction(leases, 100)
		
		if len(selected) != 1 {
			t.Fatalf("Expected 1 lease, got %d", len(selected))
		}
		
		if selected[0].ID != "old" {
			t.Errorf("Expected oldest lease, got %s", selected[0].ID)
		}
	})
	
	t.Run("IgnoreNonReclaimable", func(t *testing.T) {
		leases := []*memory.Lease{
			{ID: "reclaimable", Size: 100, Reclaimable: true, Priority: memory.PriorityNormal, LastUsed: now},
			{ID: "non-reclaimable", Size: 100, Reclaimable: false, Priority: memory.PriorityNormal, LastUsed: now.Add(-1 * time.Hour)},
		}
		
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
		leases := []*memory.Lease{
			{ID: "small", Size: 100, Reclaimable: true, LastUsed: now},
			{ID: "large", Size: 500, Reclaimable: true, LastUsed: now},
		}
		
		selected := policy.SelectForEviction(leases, 250)
		
		if selected[0].ID != "large" {
			t.Errorf("Expected largest lease, got %s", selected[0].ID)
		}
	})
}
