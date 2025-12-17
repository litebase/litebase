package cmd_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cli/config"
	"go.yaml.in/yaml/v4"
)

func TestConfigInitWithStorageCredentials(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.yml")

		// Test with all storage credentials provided
		err := cli.Run(
			"config", "init",
			"--path", configPath,
			"--cluster-id", "storage-test",
			"--port", "9876",
			"--storage-object-mode", "object",
			"--storage-bucket", "my-bucket",
			"--storage-endpoint", "s3.amazonaws.com",
			"--storage-region", "us-east-1",
			"--storage-access-key-id", "test-access-key",
			"--storage-secret-access-key", "test-secret-key",
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Verify config file was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created")
		}

		// Read and verify the config file contents
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var cfg config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &cfg); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if cfg.Server.StorageObjectMode != "object" {
			t.Errorf("expected storage_object_mode to be 'object', got %v", cfg.Server.StorageObjectMode)
		}

		if cfg.Server.StorageBucket != "my-bucket" {
			t.Errorf("expected storage_bucket to be 'my-bucket', got %v", cfg.Server.StorageBucket)
		}

		if cfg.Server.StorageEndpoint != "s3.amazonaws.com" {
			t.Errorf("expected storage_endpoint to be 's3.amazonaws.com', got %v", cfg.Server.StorageEndpoint)
		}

		if cfg.Server.StorageRegion != "us-east-1" {
			t.Errorf("expected storage_region to be 'us-east-1', got %v", cfg.Server.StorageRegion)
		}

		if cfg.Server.StorageAccessKeyId != "test-access-key" {
			t.Errorf("expected storage_access_key_id to be 'test-access-key', got %v", cfg.Server.StorageAccessKeyId)
		}

		if cfg.Server.StorageSecretAccessKey != "test-secret-key" {
			t.Errorf("expected storage_secret_access_key to be 'test-secret-key', got %v", cfg.Server.StorageSecretAccessKey)
		}
	})
}

func TestConfigInitMissingStorageCredentials(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.yml")

		// Test with storage-object-mode set to 'object' but missing credentials
		err := cli.Run(
			"config", "init",
			"--path", configPath,
			"--cluster-id", "storage-test",
			"--port", "9876",
			"--storage-object-mode", "object",
		)

		if err == nil {
			t.Fatal("expected error for missing storage credentials, got nil")
		}

		// Verify the error message mentions storage bucket (allowing for case variations)
		errMsg := err.Error()
		if errMsg != "--storage-bucket is required when storage-object-mode is 'object'" &&
			errMsg != "--Storage-Bucket is required when storage-object-mode is 'object'" {
			t.Errorf("expected error about storage bucket, got: %v", err)
		}
	})
}

func TestConfigInitLocalStorageMode(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.yml")

		// Test with local storage mode (should not require S3 credentials)
		err := cli.Run(
			"config", "init",
			"--path", configPath,
			"--cluster-id", "local-test",
			"--port", "9876",
			"--storage-object-mode", "local",
		)

		if err != nil {
			t.Fatalf("expected no error for local storage mode, got %v", err)
		}

		// Verify config file was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created")
		}

		// Read and verify the config file contents
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var cfg config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &cfg); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if cfg.Server.StorageObjectMode != "local" {
			t.Errorf("expected storage_object_mode to be 'local', got %v", cfg.Server.StorageObjectMode)
		}

		// Verify that storage credentials are not set (they're optional for local mode)
		if cfg.Server.StorageBucket != "" {
			t.Errorf("expected storage_bucket to be empty for local mode, got %v", cfg.Server.StorageBucket)
		}
	})
}
