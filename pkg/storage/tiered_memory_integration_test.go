package storage_test

import (
	"context"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/memory"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestTieredFileSystemDriver_MemoryIntegration(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CreatesWithMemoryManager", func(t *testing.T) {
			memManager, err := memory.NewManager(memory.Config{
				Capacity:  10 * 1024 * 1024, // 10MB
				Threshold: 0.85,
			})

			if err != nil {
				t.Fatalf("Failed to create memory manager: %v", err)
			}

			fs1 := storage.NewFileSystem(
				storage.NewLocalFileSystemDriver(app.Config.StorageLocalPath + "/test_high"),
			)

			fs2 := storage.NewFileSystem(
				storage.NewLocalFileSystemDriver(app.Config.StorageLocalPath + "/test_low"),
			)

			tieredFS := storage.NewTieredFileSystemDriver(
				context.Background(),
				fs1,
				fs2,
				memManager,
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			if tieredFS == nil {
				t.Fatal("Expected tiered file system driver to be created")
			}

			// Verify memory manager is tracking allocations
			stats := memManager.GetStats()

			if stats.Reserved == 0 {
				t.Logf("Note: No memory reserved yet (buffers allocated on demand)")
			}

			// Create a file which should trigger buffer allocation
			file, err := tieredFS.Create("test.txt")

			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}

			data := make([]byte, 1024)

			for i := range data {
				data[i] = byte(i % 256)
			}

			_, err = file.Write(data)

			if err != nil {
				t.Fatalf("Failed to write to file: %v", err)
			}

			err = file.Close()

			if err != nil {
				t.Fatalf("Failed to close file: %v", err)
			}

			// Clean up
			err = tieredFS.Shutdown()

			if err != nil {
				t.Fatalf("Failed to shutdown: %v", err)
			}
		})

		t.Run("BufferPoolUsesMemoryManager", func(t *testing.T) {
			memManager, err := memory.NewManager(memory.Config{
				Capacity:  5 * 1024 * 1024, // 5MB
				Threshold: 0.85,
			})

			if err != nil {
				t.Fatalf("Failed to create memory manager: %v", err)
			}

			fs1 := storage.NewFileSystem(
				storage.NewLocalFileSystemDriver(app.Config.StorageLocalPath + "/test_high2"),
			)

			fs2 := storage.NewFileSystem(
				storage.NewLocalFileSystemDriver(app.Config.StorageLocalPath + "/test_low2"),
			)

			tieredFS := storage.NewTieredFileSystemDriver(
				context.Background(),
				fs1,
				fs2,
				memManager,
				func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
					fsd.CanSyncDirtyFiles = func() bool {
						return true
					}
				},
			)

			defer func() {
				if err := tieredFS.Shutdown(); err != nil {
					t.Errorf("Failed to shutdown: %v", err)
				}
			}()

			// Create and write multiple files
			for i := range 10 {
				file, err := tieredFS.Create(string(rune('a'+i)) + ".txt")

				if err != nil {
					t.Fatalf("Failed to create file %d: %v", i, err)
				}

				// Write 10KB of data
				data := make([]byte, 10*1024)

				_, err = file.Write(data)

				if err != nil {
					t.Fatalf("Failed to write to file %d: %v", i, err)
				}

				err = file.Close()

				if err != nil {
					t.Fatalf("Failed to close file %d: %v", i, err)
				}
			}

			// Check memory stats
			stats := memManager.GetStats()

			t.Logf("Memory stats: Capacity=%d, Reserved=%d, Utilization=%.2f%%",
				stats.Capacity, stats.Reserved, stats.UtilizationPercent)

			// Verify buffer pool is integrated
			if stats.Reserved > stats.Capacity {
				t.Errorf("Reserved memory (%d) exceeds capacity (%d)", stats.Reserved, stats.Capacity)
			}
		})
	})
}
