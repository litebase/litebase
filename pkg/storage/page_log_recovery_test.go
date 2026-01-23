package storage_test

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

// TestEncryptedDatabasePageLogRecoveryAcrossRestarts tests that encrypted databases
// can recover from page logs after an unclean shutdown (shutdown before compaction)
func TestEncryptedDatabasePageLogRecoveryAcrossRestarts(t *testing.T) {
	// Generate a fixed encryption key for both servers
	dataKey := make([]byte, 32)
	_, err := rand.Read(dataKey)

	if err != nil {
		t.Fatalf("Failed to generate encryption key: %v", err)
	}

	keyHash := sha256.Sum256(dataKey)
	keyHashHex := hex.EncodeToString(keyHash[:])

	// Generate a unique bucket name for this test
	bucketName := fmt.Sprintf("test-encrypted-page-log-%d", time.Now().UnixNano())

	t.Setenv("LITEBASE_TEST_ENCRYPTION_KEY", hex.EncodeToString(dataKey))
	t.Setenv("LITEBASE_FAKE_OBJECT_STORAGE", "true")
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
	t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)

	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Setup the first server
	app1, dataPath := test.Setup(t)

	// Ensure the encryption key is set in the config
	app1.Config.DataEncryptionKey = dataKey
	app1.Config.DataEncryptionKeyHash = keyHashHex

	// Create an encrypted database
	db, err := app1.DatabaseManager.Create("encrypted_db", "main")

	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Enable encryption on the branch
	branch, err := db.PrimaryBranch()

	if err != nil {
		t.Fatalf("Failed to get primary branch: %v", err)
	}

	err = branch.SetEncryptionSettings(true, keyHashHex)

	if err != nil {
		t.Fatalf("Failed to set encryption settings: %v", err)
	}

	// Get a database connection
	dbConn, err := app1.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

	if err != nil {
		t.Fatalf("Failed to get database connection: %v", err)
	}

	conn := dbConn.GetConnection()

	// Create a table and insert data
	_, err = conn.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)", nil)

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple rows to ensure page logs are created
	for i := 1; i <= 100; i++ {
		_, err = conn.Exec(fmt.Sprintf("INSERT INTO test_table (id, value) VALUES (%d, 'test_value')", i), nil)

		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Force a checkpoint to flush WAL to page logs
	err = conn.Checkpoint()

	if err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}

	// Release the connection
	app1.DatabaseManager.ConnectionManager().Release(dbConn)

	// Shutdown the first server WITHOUT compaction
	// This simulates an unclean shutdown where page logs remain on disk
	app1.DatabaseManager.ConnectionManager().Shutdown()

	err = app1.DatabaseManager.ShutdownResources()

	if err != nil {
		t.Fatalf("Failed to shutdown database manager resources: %v", err)
	}

	err = app1.Cluster.Node().Shutdown()

	if err != nil {
		t.Fatalf("Failed to shutdown cluster node: %v", err)
	}

	// Wait a moment to ensure all resources are released
	time.Sleep(100 * time.Millisecond)

	// Now start a second server with the same data path
	// This simulates a server restart
	app2 := server.NewApp(app1.Config, nil)

	// Start the node
	<-app2.Cluster.Node().Start()

	// Ensure the encryption key is set in the config
	app2.Config.DataEncryptionKey = dataKey
	app2.Config.DataEncryptionKeyHash = keyHashHex

	// Try to access the database - this should successfully recover from page logs
	db2, err := app2.DatabaseManager.Get(db.DatabaseID)

	if err != nil {
		t.Fatalf("Failed to get database after restart: %v", err)
	}

	branch2, err := db2.PrimaryBranch()

	if err != nil {
		t.Fatalf("Failed to get primary branch after restart: %v", err)
	}

	// Get a database connection
	dbConn2, err := app2.DatabaseManager.ConnectionManager().Get(db2.DatabaseID, branch2.DatabaseBranchID)

	if err != nil {
		t.Fatalf("Failed to get database connection after restart: %v", err)
	}

	conn2 := dbConn2.GetConnection()

	// Query the data - this should work if page log recovery is successful
	result, err := conn2.Exec("SELECT COUNT(*) FROM test_table", nil)

	if err != nil {
		t.Fatalf("Failed to query data after restart: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatalf("Expected rows, got none")
	}

	// COUNT() returns INTEGER, so use Int64()
	count := result.Rows[0][0].Int64()

	if count != 100 {
		t.Fatalf("Expected 100 rows, got %d", count)
	}

	t.Logf("✓ Successfully recovered encrypted database from page logs - found %d rows", count)

	// Verify we can read specific values
	result, err = conn2.Exec("SELECT value FROM test_table WHERE id = 50", nil)

	if err != nil {
		t.Fatalf("Failed to query specific row after restart: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatalf("Expected row, got none")
	}

	value := string(result.Rows[0][0].Text())

	if value != "test_value" {
		t.Fatalf("Expected 'test_value', got '%s'", value)
	}

	// Release the connection
	app2.DatabaseManager.ConnectionManager().Release(dbConn2)

	// Cleanup the second server
	app2.DatabaseManager.ConnectionManager().Shutdown()

	err = app2.DatabaseManager.ShutdownResources()

	if err != nil {
		t.Fatalf("Failed to shutdown database manager resources: %v", err)
	}

	err = app2.Cluster.Node().Shutdown()

	if err != nil {
		t.Fatalf("Failed to shutdown cluster node: %v", err)
	}

	// The test.Teardown will clean up the dataPath
	test.Teardown(t, dataPath, nil)
}

// TestNonEncryptedDatabasePageLogRecoveryAcrossRestarts tests that non-encrypted databases
// can also recover from page logs after restart
func TestNonEncryptedDatabasePageLogRecoveryAcrossRestarts(t *testing.T) {
	// Generate a unique bucket name for this test
	bucketName := fmt.Sprintf("test-nonencrypted-page-log-%d", time.Now().UnixNano())

	// Ensure no encryption key is set
	t.Setenv("LITEBASE_TEST_ENCRYPTION_KEY", "")
	t.Setenv("LITEBASE_FAKE_OBJECT_STORAGE", "true")
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
	t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)

	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Setup the first server
	app1, dataPath := test.Setup(t)

	// Create a non-encrypted database
	db, err := app1.DatabaseManager.Create("nonencrypted_db", "main")

	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	branch, err := db.PrimaryBranch()

	if err != nil {
		t.Fatalf("Failed to get primary branch: %v", err)
	}

	// Get a database connection
	dbConn, err := app1.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

	if err != nil {
		t.Fatalf("Failed to get database connection: %v", err)
	}

	conn := dbConn.GetConnection()

	// Create a table and insert data
	_, err = conn.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)", nil)

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple rows
	for i := 1; i <= 100; i++ {
		_, err = conn.Exec(fmt.Sprintf("INSERT INTO test_table (id, value) VALUES (%d, 'test_value')", i), nil)

		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Force a checkpoint to flush WAL to page logs
	err = conn.Checkpoint()

	if err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}

	// Release the connection
	app1.DatabaseManager.ConnectionManager().Release(dbConn)

	// Shutdown the first server WITHOUT compaction
	app1.DatabaseManager.ConnectionManager().Shutdown()

	err = app1.DatabaseManager.ShutdownResources()

	if err != nil {
		t.Fatalf("Failed to shutdown database manager resources: %v", err)
	}

	err = app1.Cluster.Node().Shutdown()

	if err != nil {
		t.Fatalf("Failed to shutdown cluster node: %v", err)
	}

	// Wait a moment to ensure all resources are released
	time.Sleep(100 * time.Millisecond)

	// Now start a second server
	app2 := server.NewApp(app1.Config, nil)

	// Start the node
	<-app2.Cluster.Node().Start()

	// Try to access the database
	db2, err := app2.DatabaseManager.Get(db.DatabaseID)

	if err != nil {
		t.Fatalf("Failed to get database after restart: %v", err)
	}

	branch2, err := db2.PrimaryBranch()

	if err != nil {
		t.Fatalf("Failed to get primary branch after restart: %v", err)
	}

	// Get a database connection
	dbConn2, err := app2.DatabaseManager.ConnectionManager().Get(db2.DatabaseID, branch2.DatabaseBranchID)

	if err != nil {
		t.Fatalf("Failed to get database connection after restart: %v", err)
	}

	conn2 := dbConn2.GetConnection()

	// Query the data
	result, err := conn2.Exec("SELECT COUNT(*) FROM test_table", nil)

	if err != nil {
		t.Fatalf("Failed to query data after restart: %v", err)
	}

	if len(result.Rows) == 0 {
		t.Fatalf("Expected rows, got none")
	}

	count := result.Rows[0][0].Int64()

	if count != 100 {
		t.Fatalf("Expected 100 rows, got %d", count)
	}

	t.Logf("✓ Successfully recovered non-encrypted database from page logs - found %d rows", count)

	// Release the connection
	app2.DatabaseManager.ConnectionManager().Release(dbConn2)

	// Cleanup the second server
	app2.DatabaseManager.ConnectionManager().Shutdown()

	err = app2.DatabaseManager.ShutdownResources()

	if err != nil {
		t.Fatalf("Failed to shutdown database manager resources: %v", err)
	}

	err = app2.Cluster.Node().Shutdown()

	if err != nil {
		t.Fatalf("Failed to shutdown cluster node: %v", err)
	}

	test.Teardown(t, dataPath, nil)
}

// TestEncryptedDatabasePageLogRecoveryWithKeyMismatch tests that opening an encrypted
// database with the wrong key fails gracefully
func TestEncryptedDatabasePageLogRecoveryWithKeyMismatch(t *testing.T) {
	// Generate two different encryption keys
	dataKey1 := make([]byte, 32)
	_, err := rand.Read(dataKey1)

	if err != nil {
		t.Fatalf("Failed to generate encryption key 1: %v", err)
	}

	keyHash1 := sha256.Sum256(dataKey1)
	keyHashHex1 := hex.EncodeToString(keyHash1[:])

	dataKey2 := make([]byte, 32)
	_, err = rand.Read(dataKey2)

	if err != nil {
		t.Fatalf("Failed to generate encryption key 2: %v", err)
	}

	// Generate a unique bucket name for this test
	bucketName := fmt.Sprintf("test-key-mismatch-%d", time.Now().UnixNano())

	t.Setenv("LITEBASE_TEST_ENCRYPTION_KEY", hex.EncodeToString(dataKey1))
	t.Setenv("LITEBASE_FAKE_OBJECT_STORAGE", "true")
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
	t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)

	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Setup the first server with key1
	app1, dataPath := test.Setup(t)
	app1.Config.DataEncryptionKey = dataKey1
	app1.Config.DataEncryptionKeyHash = keyHashHex1

	// Create an encrypted database
	db, err := app1.DatabaseManager.Create("encrypted_db_mismatch", "main")

	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	branch, err := db.PrimaryBranch()

	if err != nil {
		t.Fatalf("Failed to get primary branch: %v", err)
	}

	err = branch.SetEncryptionSettings(true, keyHashHex1)

	if err != nil {
		t.Fatalf("Failed to set encryption settings: %v", err)
	}

	// Get a database connection and write data
	dbConn, err := app1.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

	if err != nil {
		t.Fatalf("Failed to get database connection: %v", err)
	}

	conn := dbConn.GetConnection()

	_, err = conn.Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)", nil)

	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = conn.Exec("INSERT INTO test_table (id, value) VALUES (1, 'test')", nil)

	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Force checkpoint
	err = conn.Checkpoint()

	if err != nil {
		t.Fatalf("Failed to checkpoint: %v", err)
	}

	app1.DatabaseManager.ConnectionManager().Release(dbConn)

	// Shutdown the first server
	app1.DatabaseManager.ConnectionManager().Shutdown()

	err = app1.DatabaseManager.ShutdownResources()

	if err != nil {
		t.Fatalf("Failed to shutdown database manager resources: %v", err)
	}

	err = app1.Cluster.Node().Shutdown()

	if err != nil {
		t.Fatalf("Failed to shutdown cluster node: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Try to start server with wrong key (key2)
	app2Config := app1.Config
	app2Config.DataEncryptionKey = dataKey2

	app2 := server.NewApp(app2Config, nil)

	<-app2.Cluster.Node().Start()

	// Try to access the database - this should fail
	_, err = app2.DatabaseManager.Get(db.DatabaseID)

	if err != nil {
		// Expected to fail - database metadata might still load but page log opening will fail
		t.Logf("Expected error when accessing database with wrong key: %v", err)
	}

	// Even if database loads, getting a connection should fail
	branch2, err := db.PrimaryBranch()

	if err == nil {
		dbConn2, err := app2.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch2.DatabaseBranchID)

		if err != nil {
			t.Logf("Expected error when getting connection with wrong key: %v", err)
		} else {
			// Try to query - this should fail
			conn2 := dbConn2.GetConnection()

			result, err := conn2.Exec("SELECT COUNT(*) FROM test_table", nil)

			if err != nil && err != sql.ErrNoRows {
				t.Logf("Expected error when querying with wrong key: %v", err)
			} else if err == nil && len(result.Rows) > 0 {
				t.Fatalf("Expected query to fail with wrong encryption key, but it succeeded")
			}

			app2.DatabaseManager.ConnectionManager().Release(dbConn2)
		}
	}

	// Cleanup
	app2.DatabaseManager.ConnectionManager().Shutdown()

	err = app2.DatabaseManager.ShutdownResources()

	if err != nil {
		t.Logf("Error during shutdown: %v", err)
	}

	err = app2.Cluster.Node().Shutdown()

	if err != nil {
		t.Logf("Error during node shutdown: %v", err)
	}

	test.Teardown(t, dataPath, nil)
}
