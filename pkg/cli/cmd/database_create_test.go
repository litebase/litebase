package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Database") {
			t.Error("expected output to contain 'Database'")
		}

		if cli.DoesNotSee("Name") {
			t.Error("expected output to contain 'Name'")
		}

		if cli.DoesNotSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesNotSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesNotSee("URL") {
			t.Error("expected output to contain 'URL'")
		}
	})
}

func TestDatabaseCreateWithPrimaryBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test", "--primary-branch", "primary")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Database") {
			t.Error("expected output to contain 'Database'")
		}

		db, err := server.App.DatabaseManager.GetByName("test")

		if err != nil {
			t.Fatalf("expected to get database, got error: %v", err)
		}

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if primaryBranch.Name != "primary" {
			t.Errorf("expected primary branch to be 'primary', got '%s'", primaryBranch.Name)
		}
	})
}

func TestDatabaseCreateWithEncryption(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "encrypted_test", "--encrypted")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Database") {
			t.Error("expected output to contain 'Database'")
		}

		// Verify encryption is configured
		db, err := server.App.DatabaseManager.GetByName("encrypted_test")

		if err != nil {
			t.Fatalf("expected to get database, got error: %v", err)
		}

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !primaryBranch.Settings.Encrypted {
			t.Errorf("expected primary branch to be encrypted")
		}

		if primaryBranch.Settings.DataEncryptionKeyHash != server.App.Config.DataEncryptionKeyHash {
			t.Errorf("expected key hash to match server config")
		}
	})
}
