package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestPruneQueuedJobs(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system database: %v", err)
		}

		// Helper to count jobs by status
		countJobsByStatus := func(status string) int {
			var count int
			err := sysDB.QueryRow(`SELECT COUNT(*) FROM queued_jobs WHERE status = ?`, status).Scan(&count)

			if err != nil {
				t.Fatalf("failed to count %s jobs: %v", status, err)
			}

			return count
		}

		// Helper to create a job with specific status and updated_at
		createJob := func(status string, daysAgo int) {
			updatedAt := time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()
			createdAt := updatedAt
			availableAt := updatedAt

			_, err := sysDB.Exec(`
			INSERT INTO queued_jobs (queue_name, name, key, data, status, created_at, updated_at, available_at, max_attempts)
			VALUES (?, ?, ?, '{}', ?, ?, ?, ?, 3)
		`, "default", "TestJob", "test-key-"+time.Now().String(), status, createdAt, updatedAt, availableAt)

			if err != nil {
				t.Fatalf("failed to create job: %v", err)
			}
		}

		t.Run("PruneOldCompletedJobs", func(t *testing.T) {
			// Create old completed jobs (35 days ago - should be deleted)
			for range 5 {
				createJob("completed", 35)
			}

			// Create recent completed jobs (10 days ago - should be kept)
			for range 3 {
				createJob("completed", 10)
			}

			// Verify initial state
			if count := countJobsByStatus("completed"); count != 8 {
				t.Errorf("Expected 8 completed jobs before pruning, got %d", count)
			}

			// Run prune
			ctx := context.Background()
			err := app.PruneQueuedJobs(ctx)

			if err != nil {
				t.Fatalf("PruneQueuedJobs failed: %v", err)
			}

			// Verify only recent jobs remain
			if count := countJobsByStatus("completed"); count != 3 {
				t.Errorf("Expected 3 completed jobs after pruning, got %d", count)
			}
		})

		t.Run("PruneOldFailedJobs", func(t *testing.T) {
			// Create old failed jobs (40 days ago - should be deleted)
			for range 4 {
				createJob("failed", 40)
			}

			// Create recent failed jobs (5 days ago - should be kept)
			for range 2 {
				createJob("failed", 5)
			}

			// Verify initial state
			if count := countJobsByStatus("failed"); count != 6 {
				t.Errorf("Expected 6 failed jobs before pruning, got %d", count)
			}

			// Run prune
			ctx := context.Background()
			err := app.PruneQueuedJobs(ctx)

			if err != nil {
				t.Fatalf("PruneQueuedJobs failed: %v", err)
			}

			// Verify only recent jobs remain
			if count := countJobsByStatus("failed"); count != 2 {
				t.Errorf("Expected 2 failed jobs after pruning, got %d", count)
			}
		})

		t.Run("KeepPendingAndProcessingJobs", func(t *testing.T) {
			// Create old pending and processing jobs - these should NOT be deleted
			createJob("pending", 40)
			createJob("processing", 40)

			pendingBefore := countJobsByStatus("pending")
			processingBefore := countJobsByStatus("processing")

			// Run prune
			ctx := context.Background()
			err := app.PruneQueuedJobs(ctx)

			if err != nil {
				t.Fatalf("PruneQueuedJobs failed: %v", err)
			}

			// Verify pending and processing jobs were NOT deleted
			if count := countJobsByStatus("pending"); count != pendingBefore {
				t.Errorf("Expected pending jobs to remain unchanged, before=%d after=%d", pendingBefore, count)
			}

			if count := countJobsByStatus("processing"); count != processingBefore {
				t.Errorf("Expected processing jobs to remain unchanged, before=%d after=%d", processingBefore, count)
			}
		})

		t.Run("ContextCancellation", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			err := app.PruneQueuedJobs(ctx)

			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})

		t.Run("NoJobsToPrune", func(t *testing.T) {
			// Clean up all old jobs first
			_, err := sysDB.Exec(`DELETE FROM queued_jobs WHERE status IN ('completed', 'failed')`)

			if err != nil {
				t.Fatalf("Failed to clean up old jobs: %v", err)
			}

			// Create only recent jobs
			createJob("completed", 5)
			createJob("failed", 10)

			beforeCount := 0

			err = sysDB.QueryRow(`SELECT COUNT(*) FROM queued_jobs WHERE status IN ('completed', 'failed')`).Scan(&beforeCount)

			if err != nil {
				t.Fatalf("Failed to count jobs before pruning: %v", err)
			}

			// Run prune
			ctx := context.Background()
			err = app.PruneQueuedJobs(ctx)

			if err != nil {
				t.Fatalf("PruneQueuedJobs failed: %v", err)
			}

			// Verify no jobs were deleted
			afterCount := 0
			err = sysDB.QueryRow(`SELECT COUNT(*) FROM queued_jobs WHERE status IN ('completed', 'failed')`).Scan(&afterCount)

			if err != nil {
				t.Fatalf("Failed to count jobs after pruning: %v", err)
			}

			if afterCount != beforeCount {
				t.Errorf("Expected no jobs to be deleted, before=%d after=%d", beforeCount, afterCount)
			}
		})
	})
}
