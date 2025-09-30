package storage_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/litebase/litebase/pkg/storage"
)

func TestPageLogger_SizeThresholdConfiguration(t *testing.T) {
	// Test default values
	defaultSize := storage.GetPageLogSizeThreshold()
	defaultCount := storage.GetPageLogCountThreshold()

	if defaultSize != storage.DefaultPageLogSizeThreshold {
		t.Errorf("Expected default size threshold %d, got %d", storage.DefaultPageLogSizeThreshold, defaultSize)
	}

	if defaultCount != storage.DefaultPageLogCountThreshold {
		t.Errorf("Expected default count threshold %d, got %d", storage.DefaultPageLogCountThreshold, defaultCount)
	}

	// Test setting custom values
	customSize := int64(50 * 1024 * 1024) // 50MB
	customCount := 5000

	storage.SetPageLogSizeThreshold(customSize)
	storage.SetPageLogCountThreshold(customCount)

	if storage.GetPageLogSizeThreshold() != customSize {
		t.Errorf("Expected custom size threshold %d, got %d", customSize, storage.GetPageLogSizeThreshold())
	}

	if storage.GetPageLogCountThreshold() != customCount {
		t.Errorf("Expected custom count threshold %d, got %d", customCount, storage.GetPageLogCountThreshold())
	}

	// Reset to defaults
	storage.SetPageLogSizeThreshold(storage.DefaultPageLogSizeThreshold)
	storage.SetPageLogCountThreshold(storage.DefaultPageLogCountThreshold)
}

func TestPageLogger_SizeBasedCompactionIntegration(t *testing.T) {
	// Set smaller thresholds for testing
	originalSizeThreshold := storage.GetPageLogSizeThreshold()
	originalCountThreshold := storage.GetPageLogCountThreshold()

	defer func() {
		storage.SetPageLogSizeThreshold(originalSizeThreshold)
		storage.SetPageLogCountThreshold(originalCountThreshold)
	}()

	// Set small thresholds to trigger size-based compaction
	storage.SetPageLogSizeThreshold(8192) // 8KB (2 pages)
	storage.SetPageLogCountThreshold(3)   // 3 pages

	// Create a minimal test environment
	tempDir, err := os.MkdirTemp("", "page_logger_size_integration_test")

	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	defer func() {
		err := os.RemoveAll(tempDir)

		if err != nil {
			t.Errorf("Failed to remove temp dir: %v", err)
		}
	}()

	// Create file system
	driver := storage.NewLocalFileSystemDriver(tempDir)

	fs := storage.NewFileSystem(driver)

	// Create a simple mock node publisher
	mockPublisher := &SimpleNodePublisher{}

	pageLogger, err := storage.NewPageLogger("test-db", "main", fs, mockPublisher)

	if err != nil {
		t.Fatalf("Failed to create page logger: %v", err)
	}

	defer func() {
		err := pageLogger.Close()

		if err != nil {
			t.Errorf("Failed to close page logger: %v", err)
		}
	}()

	// Initially the size check flag should not be set
	if pageLogger.IsSizeCheckNeeded() {
		t.Error("Expected sizeCheckNeeded flag to be false initially")
	}

	// Test that the size check flag is set when writing
	data := make([]byte, 4096) // Use proper page size of 4096 bytes
	timestamp := int64(1234567890000)

	// Write 5 pages (exceeds count threshold of 3)
	for i := range 5 {
		_, err := pageLogger.Write(int64(i), timestamp, data)

		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	// The flag should now be set
	if !pageLogger.IsSizeCheckNeeded() {
		t.Error("Expected sizeCheckNeeded flag to be set after writing pages that exceed threshold")
	}

	// Manually check size-based compaction detection
	oversized, err := pageLogger.CheckSizeBasedCompactionNeeded()

	if err != nil {
		t.Fatalf("Failed to check size-based compaction: %v", err)
	}

	if len(oversized) == 0 {
		t.Error("Expected to detect oversized page logs, but none were found")
	} else {
		t.Logf("Detected %d oversized timestamps (expected)", len(oversized))
	}

	// Test the size-based compaction check workflow without full compaction
	// We'll test that the flag persists and detection still works after multiple writes

	// First, verify we can detect oversized logs again with the current state
	oversized2, err := pageLogger.CheckSizeBasedCompactionNeeded()

	if err != nil {
		t.Fatalf("Failed to check size-based compaction second time: %v", err)
	}

	if len(oversized2) == 0 {
		t.Error("Expected to detect oversized page logs on second check")
	}

	// Write more data to a different timestamp to test multi-timestamp detection
	timestamp2 := timestamp + 1000

	for i := 0; i < 4; i++ { // Write 4 more pages at new timestamp (also exceeds threshold)
		_, err := pageLogger.Write(int64(i+100), timestamp2, data)

		if err != nil {
			t.Fatalf("Failed to write page %d at timestamp2: %v", i, err)
		}
	}

	// Flag should still be set
	if !pageLogger.IsSizeCheckNeeded() {
		t.Error("Expected sizeCheckNeeded flag to remain set after more writes")
	}

	// Now we should detect two different timestamps that are oversized
	oversized3, err := pageLogger.CheckSizeBasedCompactionNeeded()

	if err != nil {
		t.Fatalf("Failed to check size-based compaction third time: %v", err)
	}

	if len(oversized3) < 2 {
		t.Errorf("Expected to detect at least 2 oversized timestamps, got %d", len(oversized3))
	} else {
		t.Logf("Detected %d oversized timestamps as expected", len(oversized3))
	}

	t.Logf("Size-based compaction integration test completed successfully")
}

// SimpleNodePublisher is a minimal mock implementation for testing
type SimpleNodePublisher struct{}

func (s *SimpleNodePublisher) Publish(message any) (map[string]any, map[string]error) {
	// Do nothing for this simple test
	return nil, nil
}

func (s *SimpleNodePublisher) PublishMessage(ctx context.Context, message interface{}) error {
	// Do nothing for this simple test
	return nil
}

func (s *SimpleNodePublisher) IsReplica() bool {
	// Return false for simplicity (this is a primary)
	return false
}

func (s *SimpleNodePublisher) IsPrimary() bool {
	// Return true for simplicity
	return true
}

func init() {
	// Disable logging during tests to reduce noise
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	})))
}
