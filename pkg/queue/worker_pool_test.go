package queue_test

import (
	"runtime"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

func TestWorkerPool_NewWorkerPool_DefaultWorkerCount(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(
			systemDB,
			server.App.Cluster,
			queue.WorkerPoolConfig{},
		)

		expectedCount := max(runtime.NumCPU()/2, 1)

		if pool.WorkerCount() != expectedCount {
			t.Errorf("Expected worker count %d, got %d", expectedCount, pool.WorkerCount())
		}
	})
}

func TestWorkerPool_NewWorkerPool_CustomWorkerCount(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(
			systemDB,
			server.App.Cluster,
			queue.WorkerPoolConfig{
				WorkerCount: 5,
			},
		)

		if pool.WorkerCount() != 5 {
			t.Errorf("Expected worker count 5, got %d", pool.WorkerCount())
		}
	})
}

func TestWorkerPool_StartStop(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(
			systemDB,
			server.App.Cluster,
			queue.WorkerPoolConfig{
				WorkerCount: 2,
				PrimaryOnly: false,
			},
		)

		if pool.IsStarted() {
			t.Error("Expected pool to not be started initially")
		}

		err := pool.Start()

		if err != nil {
			t.Fatalf("Failed to start worker pool: %v", err)
		}

		if !pool.IsStarted() {
			t.Error("Expected pool to be started")
		}

		err = pool.Start()

		if err == nil {
			t.Error("Expected error when starting already started pool")
		}

		pool.Stop()

		if pool.IsStarted() {
			t.Error("Expected pool to be stopped")
		}
	})
}

func TestWorkerPool_PrimaryOnly(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(
			systemDB,
			server.App.Cluster,
			queue.WorkerPoolConfig{
				WorkerCount: 2,
				PrimaryOnly: true,
			},
		)

		err := pool.Start()

		if err != nil {
			t.Fatalf("Failed to start worker pool: %v", err)
		}

		isPrimary := server.App.Cluster.Node().IsPrimary()

		if isPrimary && !pool.IsStarted() {
			t.Error("Expected pool to be started on primary node")
		}

		if !isPrimary && pool.IsStarted() {
			t.Error("Expected pool to not be started on replica node when PrimaryOnly is true")
		}

		pool.Stop()
	})
}
