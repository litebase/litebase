package memory_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

func TestReservoir(t *testing.T) {
	t.Run("Reserve", func(t *testing.T) {
		r := memory.NewReservoir(1000, 0.9)

		reserved, err := r.Reserve(500)

		if err != nil {
			t.Fatalf("Failed to reserve: %v", err)
		}

		if reserved != 500 {
			t.Errorf("Expected reserved 500, got %d", reserved)
		}
	})

	t.Run("ReserveExceedsCapacity", func(t *testing.T) {
		r := memory.NewReservoir(100, 0.9)

		_, err := r.Reserve(200)

		if err != memory.ErrNoMemory {
			t.Errorf("Expected ErrNoMemory, got %v", err)
		}
	})

	t.Run("Release", func(t *testing.T) {
		r := memory.NewReservoir(1000, 0.9)

		_, err := r.Reserve(500)

		if err != nil {
			t.Fatalf("Failed to reserve: %v", err)
		}

		r.Release(500)

		if r.GetReserved() != 0 {
			t.Errorf("Expected reserved 0, got %d", r.GetReserved())
		}
	})

	t.Run("IsUnderPressure", func(t *testing.T) {
		r := memory.NewReservoir(1000, 0.9)

		_, err := r.Reserve(950)

		if err != nil {
			t.Fatalf("Failed to reserve: %v", err)
		}

		if !r.IsUnderPressure() {
			t.Error("Expected under pressure at 95% utilization")
		}
	})
}
