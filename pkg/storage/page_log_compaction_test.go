package storage_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

// TestPageLogCompactionWithEncryption tests that page log compaction works correctly
// with encrypted databases
func TestPageLogCompactionWithEncryption(t *testing.T) {
	// Speed up compaction for testing
	defaultInterval := storage.GetPageLoggerCompactInterval()
	defer storage.SetPageLoggerCompactInterval(defaultInterval)
	storage.SetPageLoggerCompactInterval(100 * time.Millisecond)

	test.RunWithObjectStorage(t, func(app *server.App) {
		// Generate proper encryption key
		dataKey := make([]byte, 32)
		_, err := rand.Read(dataKey)

		if err != nil {
			t.Fatalf("Failed to generate encryption key: %v", err)
		}

		keyHash := sha256.Sum256(dataKey)
		keyHashHex := hex.EncodeToString(keyHash[:])

		// Set encryption key in app config before creating database
		app.Config.DataEncryptionKey = dataKey
		app.Config.DataEncryptionKeyHash = keyHashHex

		// Create an encrypted database
		db, err := app.DatabaseManager.Create("compaction_test_db", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Failed to get primary branch: %v", err)
		}

		// Enable encryption
		err = branch.SetEncryptionSettings(true, keyHashHex)

		if err != nil {
			t.Fatalf("Failed to set encryption settings: %v", err)
		}

		// Get a database connection
		dbConn, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get database connection: %v", err)
		}

		conn := dbConn.GetConnection()

		// Create a table
		_, err = conn.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert data and checkpoint multiple times to create multiple page logs
		for i := 1; i <= 5; i++ {
			_, err = conn.Exec(fmt.Sprintf("INSERT INTO test_table (id, value) VALUES (%d, 'value_%d')", i, i), nil)

			if err != nil {
				t.Fatalf("Failed to insert data: %v", err)
			}

			// Checkpoint to create page logs
			err = conn.Checkpoint()

			if err != nil {
				t.Fatalf("Failed to checkpoint: %v", err)
			}

			// Small delay between checkpoints
			time.Sleep(50 * time.Millisecond)
		}

		// Get the page logger
		resources := app.DatabaseManager.Resources(branch)
		dfs := resources.FileSystem()

		if dfs == nil {
			t.Fatal("DurableDatabaseFileSystem is nil")
		}

		pageLogger := dfs.PageLogger

		if pageLogger == nil {
			t.Fatal("PageLogger is nil")
		}

		// Count page logs before compaction
		pageLogCountBefore := countPageLogsInDirectory(t, app, db.DatabaseID, branch.DatabaseBranchID)
		t.Logf("Page logs before compaction: %d", pageLogCountBefore)

		if pageLogCountBefore == 0 {
			t.Fatal("Expected page logs to exist before compaction")
		}

		// Wait for compaction interval
		time.Sleep(200 * time.Millisecond)

		// Trigger compaction
		err = pageLogger.Compact(dfs)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		// Count page logs after compaction
		pageLogCountAfter := countPageLogsInDirectory(t, app, db.DatabaseID, branch.DatabaseBranchID)
		t.Logf("Page logs after compaction: %d", pageLogCountAfter)

		// Compaction should have completed successfully
		t.Logf("✓ Page log compaction completed: before=%d, after=%d", pageLogCountBefore, pageLogCountAfter)

		// Release the connection
		app.DatabaseManager.ConnectionManager().Release(dbConn)
	})
}

// Helper function to count page logs by scanning the directory
func countPageLogsInDirectory(t *testing.T, app *server.App, databaseID, branchID string) int {
	t.Helper()

	logDir := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(databaseID, branchID))
	files, err := app.Cluster.NetworkFS().ReadDir(logDir)

	if err != nil {
		// Directory might not exist yet
		return 0
	}

	count := 0

	for _, f := range files {
		if !f.IsDir() && len(f.Name()) > 9 && f.Name()[:9] == "PAGE_LOG_" {
			count++
		}
	}

	return count
}
