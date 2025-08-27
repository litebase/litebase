package cmd_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cli/config"
	"go.yaml.in/yaml/v4"
)

func TestConfigInit(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Create a temporary directory for testing
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "test-config.yml")

		err := cli.Run("config", "init", "--cluster-id", "test-cluster", "--port", "8080", "--path", configPath)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check that the config file was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created")
		}

		// Read and verify the config file contents
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var config config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &config); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if config.Server.ClusterID != "test-cluster" {
			t.Errorf("expected cluster_id to be 'test-cluster', got %v", config.Server.ClusterID)
		}

		if config.Server.Port != "8080" {
			t.Errorf("expected port to be '8080', got %v", config.Server.Port)
		}
	})
}

func TestConfigInitWithDefaultPath(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Test with default path (should use ~/.litebase/config.yml)
		err := cli.Run("config", "init", "--cluster-id", "default-cluster", "--port", "9000")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check that the config file was created at the default path
		homeDir, err := os.UserHomeDir()

		if err != nil {
			t.Fatalf("failed to get user home directory: %v", err)
		}

		configPath := filepath.Join(homeDir, ".litebase", "config.yml")

		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created at default path")
		}

		// Read and verify the config file contents
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var config *config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &config); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if config.Server.ClusterID != "default-cluster" {
			t.Errorf("expected cluster_id to be 'default-cluster', got %v", config.Server.ClusterID)
		}

		if config.Server.Port != "9000" {
			t.Errorf("expected port to be '9000', got %v", config.Server.Port)
		}

		// Clean up
		if err := os.RemoveAll(filepath.Dir(configPath)); err != nil {
			t.Errorf("failed to clean up test config directory: %v", err)
		}
	})
}

func TestConfigInitMinimalFlags(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Create a temporary directory for testing
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "minimal-config.yml")

		// Test with minimal flags (just cluster-id)
		err := cli.Run("config", "init", "--path", configPath, "--cluster-id", "minimal-cluster")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check that the config file was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created")
		}

		// Read and verify the config file contents
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var config *config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &config); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if config.Server.ClusterID != "minimal-cluster" {
			t.Errorf("expected cluster_id to be 'minimal-cluster', got %v", config.Server.ClusterID)
		}

		// Port should be empty when not provided (flag overrides default)
		if config.Server.Port != "9876" {
			t.Errorf("expected port to be empty when not provided, got %v", config.Server.Port)
		}
	})
}

func TestConfigInitWithAllFlags(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Create a temporary directory for testing
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "full-config.yml")

		// Test with all available flags
		err := cli.Run("config", "init",
			"--path", configPath,
			"--cluster-id", "full-cluster",
			"--port", "8090",
			"--debug",
			"--key", "test-key",
			"--storage-path", "/test/data",
			"--storage-network-path", "/test/network",
			"--storage-tmp-path", "/test/tmp",
			"--tls-cert-path", "/test/cert.pem",
			"--tls-key-path", "/test/key.pem",
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check that the config file was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected config file to be created")
		}

		// Read and verify the config file contents
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var config *config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &config); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if config.Server.ClusterID != "full-cluster" {
			t.Errorf("expected cluster_id to be 'full-cluster', got %v", config.Server.ClusterID)
		}

		if config.Server.Port != "8090" {
			t.Errorf("expected port to be '8090', got %v", config.Server.Port)
		}

		if config.Server.Debug != true {
			t.Errorf("expected debug to be true, got %v", config.Server.Debug)
		}

		if config.Server.Key != "test-key" {
			t.Errorf("expected key to be 'test-key', got %v", config.Server.Key)
		}

		if config.Server.StoragePath != "/test/data" {
			t.Errorf("expected storage_path to be '/test/data', got %v", config.Server.StoragePath)
		}

		if config.Server.StorageNetworkPath != "/test/network" {
			t.Errorf("expected storage_network_path to be '/test/network', got %v", config.Server.StorageNetworkPath)
		}

		if config.Server.StorageTmpPath != "/test/tmp" {
			t.Errorf("expected storage_tmp_path to be '/test/tmp', got %v", config.Server.StorageTmpPath)
		}

		if config.Server.TLSCertPath != "/test/cert.pem" {
			t.Errorf("expected tls_cert_path to be '/test/cert.pem', got %v", config.Server.TLSCertPath)
		}

		if config.Server.TLSKeyPath != "/test/key.pem" {
			t.Errorf("expected tls_key_path to be '/test/key.pem', got %v", config.Server.TLSKeyPath)
		}
	})
}
