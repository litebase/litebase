package vector_test

import (
	"sync"
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

// MockIndexManager implements VectorIndexManagerInterface for testing
type MockIndexManager struct {
	mu      sync.Mutex
	pending map[string]int
}

func NewMockIndexManager() *MockIndexManager {
	return &MockIndexManager{
		pending: make(map[string]int),
	}
}

func (m *MockIndexManager) MarkPending(databaseID, branchID, tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := databaseID + ":" + branchID + ":" + tableName
	m.pending[key]++
}

// ProcessInline is a no-op in the test mock (satisfies VectorIndexManagerInterface).
func (m *MockIndexManager) ProcessInline(databaseID, branchID, tableName string) {}

// RunSplits is a no-op in the test mock (satisfies VectorIndexManagerInterface).
func (m *MockIndexManager) RunSplits(databaseID, branchID, tableName string) {}

func (m *MockIndexManager) GetPendingCount(databaseID, branchID, tableName string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := databaseID + ":" + branchID + ":" + tableName

	return m.pending[key]
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

func TestNotifyVectorInsert(t *testing.T) {
	// Save original and restore after test
	original := vector.GetGlobalIndexManager()
	defer vector.SetGlobalIndexManager(original)

	t.Run("NotifyWithManager", func(t *testing.T) {
		mock := NewMockIndexManager()
		vector.SetGlobalIndexManager(mock)

		vector.NotifyVectorInsert("db1", "branch1", "table1")

		count := mock.GetPendingCount("db1", "branch1", "table1")

		if count != 1 {
			t.Errorf("Expected pending count=1, got %d", count)
		}
	})

	t.Run("NotifyMultipleTimes", func(t *testing.T) {
		mock := NewMockIndexManager()
		vector.SetGlobalIndexManager(mock)

		vector.NotifyVectorInsert("db1", "branch1", "table1")
		vector.NotifyVectorInsert("db1", "branch1", "table1")
		vector.NotifyVectorInsert("db1", "branch1", "table1")

		count := mock.GetPendingCount("db1", "branch1", "table1")

		if count != 3 {
			t.Errorf("Expected pending count=3, got %d", count)
		}
	})

	t.Run("NotifyDifferentIndexes", func(t *testing.T) {
		mock := NewMockIndexManager()
		vector.SetGlobalIndexManager(mock)

		vector.NotifyVectorInsert("db1", "branch1", "table1")
		vector.NotifyVectorInsert("db1", "branch1", "table2")
		vector.NotifyVectorInsert("db1", "branch2", "table1")
		vector.NotifyVectorInsert("db2", "branch1", "table1")

		count1 := mock.GetPendingCount("db1", "branch1", "table1")
		count2 := mock.GetPendingCount("db1", "branch1", "table2")
		count3 := mock.GetPendingCount("db1", "branch2", "table1")
		count4 := mock.GetPendingCount("db2", "branch1", "table1")

		if count1 != 1 || count2 != 1 || count3 != 1 || count4 != 1 {
			t.Errorf("Expected all counts=1, got %d, %d, %d, %d", count1, count2, count3, count4)
		}
	})

	t.Run("NotifyWithoutManager", func(t *testing.T) {
		vector.SetGlobalIndexManager(nil)

		// Should not panic when manager is nil
		vector.NotifyVectorInsert("db1", "branch1", "table1")
	})
}

func TestNotifierConcurrency(t *testing.T) {
	// Save original and restore after test
	original := vector.GetGlobalIndexManager()
	defer vector.SetGlobalIndexManager(original)

	t.Run("ConcurrentNotify", func(t *testing.T) {
		mock := NewMockIndexManager()
		vector.SetGlobalIndexManager(mock)

		var wg sync.WaitGroup
		goroutines := 100

		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				vector.NotifyVectorInsert("db1", "branch1", "table1")
			}()
		}

		wg.Wait()

		count := mock.GetPendingCount("db1", "branch1", "table1")

		if count != goroutines {
			t.Errorf("Expected pending count=%d, got %d", goroutines, count)
		}
	})

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

	t.Run("ConcurrentNotifyDifferentIndexes", func(t *testing.T) {
		mock := NewMockIndexManager()
		vector.SetGlobalIndexManager(mock)

		var wg sync.WaitGroup
		goroutines := 25

		wg.Add(goroutines * 4)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				vector.NotifyVectorInsert("db1", "branch1", "table1")
			}()

			go func() {
				defer wg.Done()
				vector.NotifyVectorInsert("db1", "branch1", "table2")
			}()

			go func() {
				defer wg.Done()
				vector.NotifyVectorInsert("db1", "branch2", "table1")
			}()

			go func() {
				defer wg.Done()
				vector.NotifyVectorInsert("db2", "branch1", "table1")
			}()
		}

		wg.Wait()

		count1 := mock.GetPendingCount("db1", "branch1", "table1")
		count2 := mock.GetPendingCount("db1", "branch1", "table2")
		count3 := mock.GetPendingCount("db1", "branch2", "table1")
		count4 := mock.GetPendingCount("db2", "branch1", "table1")

		if count1 != goroutines || count2 != goroutines || count3 != goroutines || count4 != goroutines {
			t.Errorf("Expected all counts=%d, got %d, %d, %d, %d", goroutines, count1, count2, count3, count4)
		}
	})
}

func TestNotifierIsolation(t *testing.T) {
	// Save original and restore after test
	original := vector.GetGlobalIndexManager()
	defer vector.SetGlobalIndexManager(original)

	t.Run("ManagersAreIsolated", func(t *testing.T) {
		mock1 := NewMockIndexManager()
		mock2 := NewMockIndexManager()

		vector.SetGlobalIndexManager(mock1)
		vector.NotifyVectorInsert("db1", "branch1", "table1")

		count1 := mock1.GetPendingCount("db1", "branch1", "table1")

		if count1 != 1 {
			t.Errorf("Expected mock1 count=1, got %d", count1)
		}

		vector.SetGlobalIndexManager(mock2)
		vector.NotifyVectorInsert("db1", "branch1", "table1")

		count2 := mock2.GetPendingCount("db1", "branch1", "table1")

		if count2 != 1 {
			t.Errorf("Expected mock2 count=1, got %d", count2)
		}

		// Original manager should still have its count
		if count1 != 1 {
			t.Errorf("Expected mock1 to retain count=1, got %d", count1)
		}
	})
}
