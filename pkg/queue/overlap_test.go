package queue_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

func TestWorker_WithoutOverlapping(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(systemDB, server.App.Cluster, queue.WorkerPoolConfig{
			WorkerCount: 2,
		})

		var executionOrder []string
		var mu sync.Mutex
		executingKeys := make(map[string]bool)

		handler := func(ctx context.Context, data map[string]any) error {
			key := data["key"].(string)

			mu.Lock()

			executingKeys[key] = true
			executionOrder = append(executionOrder, key)
			mu.Unlock()

			time.Sleep(500 * time.Millisecond)

			mu.Lock()
			delete(executingKeys, key)
			mu.Unlock()

			return nil
		}

		err := pool.RegisterJob(
			"OverlapTestJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(3, 5*time.Second),
			queue.WithoutOverlapping(200*time.Millisecond),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		// Use the pool's dispatcher which knows about the registered jobs
		dispatcher := pool.NewDispatcher()

		_, err = dispatcher.DispatchJob("OverlapTestJob", map[string]any{
			"key": "same-key",
		}, queue.WithKey("same-key"))

		if err != nil {
			t.Fatalf("Failed to dispatch job 1: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		_, err = dispatcher.DispatchJob("OverlapTestJob", map[string]any{
			"key": "same-key",
		}, queue.WithKey("same-key"))

		if err != nil {
			t.Fatalf("Failed to dispatch job 2: %v", err)
		}

		time.Sleep(2 * time.Second)

		mu.Lock()
		defer mu.Unlock()

		if len(executionOrder) != 2 {
			t.Errorf("Expected 2 executions, got %d", len(executionOrder))
		}
	})
}

func TestWorker_WithoutOverlapping_DifferentKeys(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(systemDB, server.App.Cluster, queue.WorkerPoolConfig{
			WorkerCount: 2,
		})

		var executionCount sync.WaitGroup
		executionCount.Add(2)

		handler := func(ctx context.Context, data map[string]any) error {
			time.Sleep(200 * time.Millisecond)
			executionCount.Done()

			return nil
		}

		err := pool.RegisterJob(
			"DifferentKeysJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(3, 5*time.Second),
			queue.WithoutOverlapping(),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		// Use the pool's dispatcher which knows about the registered jobs
		dispatcher := pool.NewDispatcher()

		_, err = dispatcher.DispatchJob("DifferentKeysJob", map[string]any{
			"key": "key-1",
		}, queue.WithKey("key-1"))

		if err != nil {
			t.Fatalf("Failed to dispatch job 1: %v", err)
		}

		_, err = dispatcher.DispatchJob("DifferentKeysJob", map[string]any{
			"key": "key-2",
		}, queue.WithKey("key-2"))

		if err != nil {
			t.Fatalf("Failed to dispatch job 2: %v", err)
		}

		done := make(chan struct{})

		go func() {
			executionCount.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success - both jobs completed within timeout
		case <-time.After(5 * time.Second):
			t.Error("Jobs with different keys should execute concurrently (within 5s)")
		}
	})
}
