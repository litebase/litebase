package storage_test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

// TestEncryptedDatabaseWAL tests that WAL files are properly encrypted when database encryption is enabled
func TestEncryptedDatabaseWAL(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Generate a 32-byte encryption key
		dataKey := make([]byte, 32)
		_, err := rand.Read(dataKey)

		if err != nil {
			t.Fatalf("Failed to generate encryption key: %v", err)
		}

		keyHash := sha256.Sum256(dataKey)
		keyHashHex := hex.EncodeToString(keyHash[:])

		// Configure the data encryption key in the app config
		app.Config.DataEncryptionKey = dataKey
		app.Config.DataEncryptionKeyHash = keyHashHex

		// Create a database
		db, err := app.DatabaseManager.Create("test_encrypted_wal", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// Get the primary branch
		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Failed to get primary branch: %v", err)
		}

		// Enable encryption on the branch
		err = branch.SetEncryptionSettings(true, keyHashHex)

		if err != nil {
			t.Fatalf("Failed to enable encryption: %v", err)
		}

		// Get the WAL manager for this database
		walManager, err := app.DatabaseManager.Resources(branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Failed to get WAL manager: %v", err)
		}

		// Create a new WAL version
		walTimestamp, err := walManager.Create()

		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}

		if walTimestamp == nil {
			t.Fatal("WAL timestamp is nil")
		}

		// Get the WAL file
		walFile, err := walTimestamp.File()

		if err != nil {
			t.Fatalf("Failed to get WAL file: %v", err)
		}

		// Verify it's an EncryptedStreamFile
		_, isEncrypted := walFile.(*storage.EncryptedStreamFile)

		if !isEncrypted {
			t.Error("Expected WAL file to be encrypted, but it's not")
		}

		// Write some data to the WAL
		testData := make([]byte, 4096)

		for i := range testData {
			testData[i] = byte(i % 256)
		}

		n, err := walFile.WriteAt(testData, 0)

		if err != nil {
			t.Fatalf("Failed to write to WAL: %v", err)
		}

		if n != 4096 {
			t.Errorf("Expected to write 4096 bytes, wrote %d", n)
		}

		// Read it back and verify
		readData := make([]byte, 4096)
		n, err = walFile.ReadAt(readData, 0)

		if err != nil {
			t.Fatalf("Failed to read from WAL: %v", err)
		}

		if n != 4096 {
			t.Errorf("Expected to read 4096 bytes, read %d", n)
		}

		// Verify data matches
		for i := range testData {
			if testData[i] != readData[i] {
				t.Errorf("Data mismatch at byte %d: expected %d, got %d", i, testData[i], readData[i])
				break
			}
		}

		// Verify the underlying file is encrypted by checking raw file content
		stat, err := walFile.Stat()

		if err != nil {
			t.Fatalf("Failed to stat WAL file: %v", err)
		}

		// Size should be header (64 bytes) + encrypted page (4096 bytes)
		expectedSize := int64(64 + 4096)

		if stat.Size() != expectedSize {
			t.Errorf("Expected file size %d, got %d", expectedSize, stat.Size())
		}
	})
}

// TestEncryptedDatabaseWALKeyMismatch tests that opening a WAL with wrong key fails
func TestEncryptedDatabaseWALKeyMismatch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Generate encryption keys
		dataKey1 := make([]byte, 32)
		dataKey2 := make([]byte, 32)
		_, err := rand.Read(dataKey1)

		if err != nil {
			t.Fatalf("Failed to generate encryption key: %v", err)
		}

		_, err = rand.Read(dataKey2)

		if err != nil {
			t.Fatalf("Failed to generate second encryption key: %v", err)
		}

		keyHash1 := sha256.Sum256(dataKey1)
		keyHash2 := sha256.Sum256(dataKey2)
		keyHashHex1 := hex.EncodeToString(keyHash1[:])
		keyHashHex2 := hex.EncodeToString(keyHash2[:])

		// Configure first key
		app.Config.DataEncryptionKey = dataKey1
		app.Config.DataEncryptionKeyHash = keyHashHex1

		// Create encrypted database
		db, err := app.DatabaseManager.Create("test_key_mismatch", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Failed to get primary branch: %v", err)
		}

		err = branch.SetEncryptionSettings(true, keyHashHex1)

		if err != nil {
			t.Fatalf("Failed to enable encryption: %v", err)
		}

		walManager, err := app.DatabaseManager.Resources(branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Failed to get WAL manager: %v", err)
		}

		walTimestamp, err := walManager.Create()

		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}

		// Write some data
		walFile, err := walTimestamp.File()

		if err != nil {
			t.Fatalf("Failed to get WAL file: %v", err)
		}

		testData := make([]byte, 4096)
		_, err = walFile.WriteAt(testData, 0)

		if err != nil {
			t.Fatalf("Failed to write to WAL: %v", err)
		}

		err = walFile.Close()

		if err != nil {
			t.Fatalf("Failed to close WAL: %v", err)
		}

		// Close the WAL object to clear cached file handle
		err = walTimestamp.Close()

		if err != nil {
			t.Fatalf("Failed to close WAL timestamp: %v", err)
		}

		// Now change the key to key2 but keep the database expecting key1
		app.Config.DataEncryptionKey = dataKey2
		app.Config.DataEncryptionKeyHash = keyHashHex2

		// Update branch to expect the wrong key
		err = branch.SetEncryptionSettings(true, keyHashHex2)

		if err != nil {
			t.Fatalf("Failed to update encryption settings: %v", err)
		}

		// Shutdown the old WAL manager to clear caches
		walManager.Shutdown()

		// Try to reopen the WAL - should fail because keyHash doesn't match
		// Get a fresh WAL manager
		walManager2, err := app.DatabaseManager.Resources(branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Failed to get new WAL manager: %v", err)
		}

		walTimestamp2, err := walManager2.Get(walTimestamp.Timestamp())

		if err != nil {
			t.Fatalf("Failed to get WAL timestamp: %v", err)
		}

		_, err = walTimestamp2.File()

		if err == nil {
			t.Error("Expected error when opening WAL with wrong key, but got nil")
		}

		if err != nil && err.Error() != "key hash mismatch: file was encrypted with different key" {
			t.Logf("Got error (expected): %v", err)
		}
	})
}

// TestEncryptedPageLog tests that PageLog files are properly encrypted
func TestEncryptedPageLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Generate encryption key
		dataKey := make([]byte, 32)
		_, err := rand.Read(dataKey)

		if err != nil {
			t.Fatalf("Failed to generate encryption key: %v", err)
		}

		keyHash := sha256.Sum256(dataKey)

		// Create an encrypted PageLog directly
		fs := app.Cluster.NetworkFS()
		path := "test_encrypted_pagelog"

		pageLog, err := storage.NewEncryptedPageLog(fs, app.Cluster.MemoryManager, path, dataKey, keyHash)

		if err != nil {
			t.Fatalf("Failed to create encrypted PageLog: %v", err)
		}

		defer func() {
			if err := pageLog.Close(); err != nil {
				t.Fatalf("Failed to close PageLog: %v", err)
			}

			if err := fs.Remove(path); err != nil {
				t.Fatalf("Failed to remove PageLog file: %v", err)
			}

			if err := fs.Remove(path + "_INDEX"); err != nil {
				t.Fatalf("Failed to remove PageLog index file: %v", err)
			}
		}()

		// Write a page
		testData := make([]byte, 4096)

		for i := range testData {
			testData[i] = byte(i % 256)
		}

		testVersion := time.Now().UnixNano()
		err = pageLog.Append(1, testVersion, testData)

		if err != nil {
			t.Fatalf("Failed to append to PageLog: %v", err)
		}

		// Read it back
		readData := make([]byte, 4096)
		found, version, err := pageLog.Get(storage.PageNumber(1), storage.PageVersion(testVersion), readData)

		if err != nil {
			t.Fatalf("Failed to read from PageLog: %v", err)
		}

		if !found {
			t.Error("Expected to find page, but didn't")
		}

		if version == 0 {
			t.Error("Expected non-zero version")
		}

		// Verify data matches
		for i := range testData {
			if testData[i] != readData[i] {
				t.Errorf("Data mismatch at byte %d: expected %d, got %d", i, testData[i], readData[i])
				break
			}
		}

		// Close and reopen to verify persistence
		err = pageLog.Close()

		if err != nil {
			t.Fatalf("Failed to close PageLog: %v", err)
		}

		pageLog2, err := storage.NewEncryptedPageLog(fs, app.Cluster.MemoryManager, path, dataKey, keyHash)

		if err != nil {
			t.Fatalf("Failed to reopen encrypted PageLog: %v", err)
		}

		defer func() {
			if err := pageLog2.Close(); err != nil {
				t.Fatalf("Failed to close reopened PageLog: %v", err)
			}
		}()

		// Read the data again
		readData2 := make([]byte, 4096)
		found2, _, err := pageLog2.Get(storage.PageNumber(1), storage.PageVersion(testVersion), readData2)

		if err != nil {
			t.Fatalf("Failed to read from reopened PageLog: %v", err)
		}

		if !found2 {
			t.Error("Expected to find page after reopen, but didn't")
		}

		// Verify data still matches
		for i := range testData {
			if testData[i] != readData2[i] {
				t.Errorf("Data mismatch after reopen at byte %d: expected %d, got %d", i, testData[i], readData2[i])
				break
			}
		}
	})
}

// TestEncryptedRange tests that Range files are properly encrypted
func TestEncryptedRange(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Generate encryption key
		dataKey := make([]byte, 32)
		_, err := rand.Read(dataKey)

		if err != nil {
			t.Fatalf("Failed to generate encryption key: %v", err)
		}

		keyHash := sha256.Sum256(dataKey)

		// Create an encrypted Range directly
		fs := app.Cluster.TieredFS()
		databaseId := "test_db"
		branchId := "main"
		rangeNumber := int64(1)
		pageSize := int64(4096)

		r, err := storage.NewEncryptedRange(databaseId, branchId, fs, rangeNumber, pageSize, dataKey, keyHash)

		if err != nil {
			t.Fatalf("Failed to create encrypted Range: %v", err)
		}

		defer func() {
			if err := r.Close(); err != nil {
				t.Fatalf("Failed to close Range: %v", err)
			}

			// Clean up the range file
			if err := fs.Remove(r.Path()); err != nil {
				t.Fatalf("Failed to remove Range file: %v", err)
			}
		}()

		// Write multiple pages
		for pageNum := int64(1); pageNum <= 5; pageNum++ {
			testData := make([]byte, 4096)

			for i := range testData {
				testData[i] = byte((i + int(pageNum)) % 256)
			}

			n, err := r.WriteAt(pageNum, testData)

			if err != nil {
				t.Fatalf("Failed to write page %d to Range: %v", pageNum, err)
			}

			if n != 4096 {
				t.Errorf("Expected to write 4096 bytes, wrote %d", n)
			}
		}

		// Read pages back and verify
		for pageNum := int64(1); pageNum <= 5; pageNum++ {
			expectedData := make([]byte, 4096)

			for i := range expectedData {
				expectedData[i] = byte((i + int(pageNum)) % 256)
			}

			readData := make([]byte, 4096)
			n, err := r.ReadAt(pageNum, readData)

			if err != nil {
				t.Fatalf("Failed to read page %d from Range: %v", pageNum, err)
			}

			if n != 4096 {
				t.Errorf("Expected to read 4096 bytes, read %d", n)
			}

			// Verify data matches
			for i := range expectedData {
				if expectedData[i] != readData[i] {
					t.Errorf("Page %d data mismatch at byte %d: expected %d, got %d", pageNum, i, expectedData[i], readData[i])
					break
				}
			}
		}

		// Verify page count
		pageCount := r.PageCount()

		if pageCount != 5 {
			t.Errorf("Expected 5 pages, got %d", pageCount)
		}

		// Close and reopen to verify persistence
		err = r.Close()

		if err != nil {
			t.Fatalf("Failed to close Range: %v", err)
		}

		r2, err := storage.NewEncryptedRange(databaseId, branchId, fs, rangeNumber, pageSize, dataKey, keyHash)

		if err != nil {
			t.Fatalf("Failed to reopen encrypted Range: %v", err)
		}

		defer func() {
			if err := r2.Close(); err != nil {
				t.Fatalf("Failed to close reopened Range: %v", err)
			}
		}()

		// Verify page count after reopen
		pageCount2 := r2.PageCount()

		if pageCount2 != 5 {
			t.Errorf("Expected 5 pages after reopen, got %d", pageCount2)
		}

		// Read pages again to verify persistence
		for pageNum := int64(1); pageNum <= 5; pageNum++ {
			expectedData := make([]byte, 4096)

			for i := range expectedData {
				expectedData[i] = byte((i + int(pageNum)) % 256)
			}

			readData := make([]byte, 4096)
			_, err := r2.ReadAt(pageNum, readData)

			if err != nil {
				t.Fatalf("Failed to read page %d after reopen: %v", pageNum, err)
			}

			// Verify data still matches
			for i := range expectedData {
				if expectedData[i] != readData[i] {
					t.Errorf("Page %d data mismatch after reopen at byte %d: expected %d, got %d", pageNum, i, expectedData[i], readData[i])
					break
				}
			}
		}
	})
}

// TestUnencryptedDatabaseWithEncryptionKeyConfigured tests backward compatibility
func TestUnencryptedDatabaseWithEncryptionKeyConfigured(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Configure a data encryption key
		dataKey := make([]byte, 32)
		_, err := rand.Read(dataKey)

		if err != nil {
			t.Fatalf("Failed to generate encryption key: %v", err)
		}

		keyHash := sha256.Sum256(dataKey)
		keyHashHex := hex.EncodeToString(keyHash[:])

		app.Config.DataEncryptionKey = dataKey
		app.Config.DataEncryptionKeyHash = keyHashHex

		// Create a database WITHOUT encryption enabled
		db, err := app.DatabaseManager.Create("test_unencrypted_db", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Failed to get primary branch: %v", err)
		}

		// Verify encryption is NOT enabled by default
		isEncrypted, err := branch.IsEncrypted()

		if err != nil {
			t.Fatalf("Failed to check encryption status: %v", err)
		}

		if isEncrypted {
			t.Error("Expected database to be unencrypted by default")
		}

		// Get WAL manager
		walManager, err := app.DatabaseManager.Resources(branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Failed to get WAL manager: %v", err)
		}

		// Create a WAL
		walTimestamp, err := walManager.Create()

		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}

		// Get the WAL file
		walFile, err := walTimestamp.File()

		if err != nil {
			t.Fatalf("Failed to get WAL file: %v", err)
		}

		// Verify it's NOT an EncryptedStreamFile
		_, isEncryptedFile := walFile.(*storage.EncryptedStreamFile)

		if isEncryptedFile {
			t.Error("Expected WAL file to be unencrypted, but it's encrypted")
		}

		// Write and read data to ensure it works
		testData := make([]byte, 4096)

		for i := range testData {
			testData[i] = byte(i % 256)
		}

		_, err = walFile.WriteAt(testData, 0)

		if err != nil {
			t.Fatalf("Failed to write to unencrypted WAL: %v", err)
		}

		readData := make([]byte, 4096)
		_, err = walFile.ReadAt(readData, 0)

		if err != nil {
			t.Fatalf("Failed to read from unencrypted WAL: %v", err)
		}

		// Verify data matches
		for i := range testData {
			if testData[i] != readData[i] {
				t.Errorf("Data mismatch at byte %d: expected %d, got %d", i, testData[i], readData[i])
				break
			}
		}
	})
}

// TestDataEncryptionKeyNotFound tests the error case when encryption key is not found
func TestDataEncryptionKeyNotFound(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Don't configure any encryption key
		app.Config.DataEncryptionKey = nil
		app.Config.DataEncryptionKeyHash = ""

		// Create a database and try to enable encryption
		db, err := app.DatabaseManager.Create("test_no_key", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Failed to get primary branch: %v", err)
		}

		// Set encryption with a fake key hash
		fakeKeyHash := "abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"
		err = branch.SetEncryptionSettings(true, fakeKeyHash)

		if err != nil {
			t.Fatalf("Failed to set encryption settings: %v", err)
		}

		// Try to get WAL manager - should work
		walManager, err := app.DatabaseManager.Resources(branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Failed to get WAL manager: %v", err)
		}

		// Try to create a WAL - should work
		walTimestamp, err := walManager.Create()

		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}

		// Try to get the file - this should fail because the key is not found
		_, err = walTimestamp.File()

		if err == nil {
			t.Error("Expected error when DataEncryptionKey is not found, but got nil")
		}

		expectedError := "DataEncryptionKey for this database not found"

		if err != nil && err.Error() != expectedError && err.Error() != "DataEncryptionKey for this database not found (hash: "+fakeKeyHash+")" {
			t.Logf("Got error message: %v", err)
		}
	})
}
