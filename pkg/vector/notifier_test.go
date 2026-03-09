package vector_test

import (
	"sync"
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

// MockIndexManager implements VectorIndexManagerInterface for testing
type MockIndexManager struct{}

func NewMockIndexManager() *MockIndexManager {
	return &MockIndexManager{}
}

// RunSplitsWithConnection is a no-op in the test mock (satisfies VectorIndexManagerInterface).
func (m *MockIndexManager) RunSplitsWithConnection(conn any, databaseID, branchID, tableName string) {
}

func TestSetAndGetGlobalIndexManager(t *testing.T) {
	// Save original and restore after test
	original := vector.GetGlobalIndexManager()
	defer vector.SetGlobalIndexManager(original)

	t.Run("SetAndGet", func(t *testing.T) {
		mock := NewMockIndexManager()

		vector.SetGlobalIndexManager(mock)

		result := vector.GetGlobalIndexManager()

		if result != mock {
			t.Error("Expected to get the same manager that was set")
		}
	})

	t.Run("GetWhenNil", func(t *testing.T) {
		vector.SetGlobalIndexManager(nil)

		result := vector.GetGlobalIndexManager()

		if result != nil {
			t.Error("Expected nil when no manager is set")
		}
	})

	t.Run("ReplaceManager", func(t *testing.T) {
		mock1 := NewMockIndexManager()
		mock2 := NewMockIndexManager()

		vector.SetGlobalIndexManager(mock1)

		result := vector.GetGlobalIndexManager()

		if result != mock1 {
			t.Error("Expected first manager")
		}

		vector.SetGlobalIndexManager(mock2)

		result = vector.GetGlobalIndexManager()

		if result != mock2 {
			t.Error("Expected second manager after replacement")
		}
	})
}

func TestGlobalIndexManagerConcurrency(t *testing.T) {
	// Save original and restore after test
	original := vector.GetGlobalIndexManager()
	defer vector.SetGlobalIndexManager(original)

	t.Run("ConcurrentSetAndGet", func(t *testing.T) {
		var wg sync.WaitGroup
		goroutines := 50

		wg.Add(goroutines * 2)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				mock := NewMockIndexManager()
				vector.SetGlobalIndexManager(mock)
			}()

			go func() {
				defer wg.Done()
				_ = vector.GetGlobalIndexManager()
			}()
		}

		wg.Wait()

		// Should not panic
		mgr := vector.GetGlobalIndexManager()

		if mgr == nil {
			t.Log("Manager is nil after concurrent set/get (expected due to race)")
		}
	})
}
