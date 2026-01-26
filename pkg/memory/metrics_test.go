package memory_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

func TestMetrics(t *testing.T) {
	t.Run("RecordAllocation", func(t *testing.T) {
		m := memory.NewMetrics()

		m.RecordAllocation(1024, "cache")

		if m.GetTotalReserved() != 1024 {
			t.Errorf("Expected total reserved 1024, got %d", m.GetTotalReserved())
		}

		byComponent := m.GetMemoryByComponent()

		if byComponent["cache"] != 1024 {
			t.Errorf("Expected cache 1024, got %d", byComponent["cache"])
		}
	})

	t.Run("RecordRelease", func(t *testing.T) {
		m := memory.NewMetrics()

		m.RecordAllocation(2048, "cache")
		m.RecordRelease(1024, "cache")

		byComponent := m.GetMemoryByComponent()

		if byComponent["cache"] != 1024 {
			t.Errorf("Expected cache 1024, got %d", byComponent["cache"])
		}
	})

	t.Run("MultipleComponents", func(t *testing.T) {
		m := memory.NewMetrics()

		m.RecordAllocation(1000, "cache")
		m.RecordAllocation(500, "buffer")

		byComponent := m.GetMemoryByComponent()

		if len(byComponent) != 2 {
			t.Errorf("Expected 2 components, got %d", len(byComponent))
		}
	})
}
