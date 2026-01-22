package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
)

func TestBranch_EncryptionSettings(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		dm := server.App.DatabaseManager

		t.Run("Default branch is not encrypted", func(t *testing.T) {
			// Create a database
			db, err := dm.Create("test_db", "main")

			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}

			// Get primary branch
			branch, err := db.PrimaryBranch()

			if err != nil {
				t.Fatalf("Failed to get primary branch: %v", err)
			}

			// Check encryption settings
			isEncrypted, err := branch.IsEncrypted()

			if err != nil {
				t.Fatalf("Failed to check encryption: %v", err)
			}

			if isEncrypted {
				t.Error("Default branch should not be encrypted")
			}

			keyHash, err := branch.GetDataEncryptionKeyHash()

			if err != nil {
				t.Fatalf("Failed to get key hash: %v", err)
			}

			if keyHash != "" {
				t.Errorf("Expected empty key hash, got: %s", keyHash)
			}
		})

		t.Run("Set encryption settings on branch", func(t *testing.T) {
			// Create a database
			db, err := dm.Create("test_encrypted_db", "main")

			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}

			// Get primary branch
			branch, err := db.PrimaryBranch()

			if err != nil {
				t.Fatalf("Failed to get primary branch: %v", err)
			}

			// Set encryption settings
			testKeyHash := "abcd1234567890abcd1234567890abcd1234567890abcd1234567890abcd1234"

			err = branch.SetEncryptionSettings(true, testKeyHash)

			if err != nil {
				t.Fatalf("Failed to set encryption settings: %v", err)
			}

			// Reload branch to verify settings were persisted
			reloadedBranch, err := db.BranchByID(branch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to reload branch: %v", err)
			}

			// Verify settings were saved
			isEncrypted, err := reloadedBranch.IsEncrypted()

			if err != nil {
				t.Fatalf("Failed to check encryption: %v", err)
			}

			if !isEncrypted {
				t.Error("Branch should be encrypted")
			}

			keyHash, err := reloadedBranch.GetDataEncryptionKeyHash()

			if err != nil {
				t.Fatalf("Failed to get key hash: %v", err)
			}

			if keyHash != testKeyHash {
				t.Errorf("Expected key hash %s, got: %s", testKeyHash, keyHash)
			}
		})

		t.Run("Child branch inherits encryption settings from parent", func(t *testing.T) {
			// Create a database
			db, err := dm.Create("test_inherit_db", "main")

			if err != nil {
				t.Fatalf("Failed to create database: %v", err)
			}

			// Get primary branch
			primaryBranch, err := db.PrimaryBranch()

			if err != nil {
				t.Fatalf("Failed to get primary branch: %v", err)
			}

			// Set encryption on primary
			testKeyHash := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

			err = primaryBranch.SetEncryptionSettings(true, testKeyHash)

			if err != nil {
				t.Fatalf("Failed to set encryption settings: %v", err)
			}

			// Create child branch directly via database.NewBranch instead of database.CreateBranch
			// to avoid snapshot requirements
			childBranch, err := database.NewBranch(dm, db.ID, primaryBranch.Name, "child")

			if err != nil {
				t.Fatalf("Failed to create child branch: %v", err)
			}

			// Set the DatabaseID so the branch can be cached properly
			childBranch.DatabaseID = db.DatabaseID

			err = database.InsertBranch(childBranch)

			if err != nil {
				t.Fatalf("Failed to insert child branch: %v", err)
			}

			// Reload to get fresh settings
			reloadedChild, err := db.BranchByID(childBranch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Failed to reload child branch: %v", err)
			}

			// Verify child inherited encryption settings
			isEncrypted, err := reloadedChild.IsEncrypted()

			if err != nil {
				t.Fatalf("Failed to check child encryption: %v", err)
			}

			if !isEncrypted {
				t.Error("Child branch should inherit encrypted=true from parent")
			}

			keyHash, err := reloadedChild.GetDataEncryptionKeyHash()

			if err != nil {
				t.Fatalf("Failed to get child key hash: %v", err)
			}

			if keyHash != testKeyHash {
				t.Errorf("Child should inherit key hash %s, got: %s", testKeyHash, keyHash)
			}
		})
	})
}
