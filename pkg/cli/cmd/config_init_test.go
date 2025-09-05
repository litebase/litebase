package cmd_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

		// Verify that an encryption key was generated or used
		if config.Server.Key == "" {
			t.Error("expected encryption key to be set")
		}
	})
}

func TestConfigInitGeneratesEncryptionKey(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Create a temporary directory for testing
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "test-config-with-key.yml")

		err := cli.Run("config", "init", "--cluster-id", "test-cluster", "--port", "8080", "--path", configPath)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
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

		// Verify that an encryption key was automatically generated
		if config.Server.Key == "" {
			t.Error("expected encryption key to be automatically generated")
		}

		// Verify the key is of expected length (64 characters)
		if len(config.Server.Key) != 64 {
			t.Errorf("expected encryption key to be 64 characters, got %d", len(config.Server.Key))
		}

		// Verify the key contains only valid characters
		validChars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		for _, char := range config.Server.Key {
			if !strings.ContainsRune(validChars, char) {
				t.Errorf("encryption key contains invalid character: %c", char)
			}
		}
	})
}

func TestConfigInitWithCustomEncryptionKey(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Create a temporary directory for testing
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "custom-key-config.yml")
		customKey := "mycustomencryptionkey123456789"

		err := cli.Run("config", "init", "--cluster-id", "test-cluster", "--port", "8080", "--key", customKey, "--path", configPath)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
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

		// Verify that the custom encryption key was used
		if config.Server.Key != customKey {
			t.Errorf("expected encryption key to be '%s', got '%s'", customKey, config.Server.Key)
		}
	})
}

func TestConfigInitWithDefaultPath(t *testing.T) {
	test.Run(t, func() {
		// Create a temporary home directory to avoid conflicts with existing config
		tempHomeDir := t.TempDir()

		// Set the HOME environment variable to our temp directory
		originalHome := os.Getenv("HOME")

		if err := os.Setenv("HOME", tempHomeDir); err != nil {
			t.Fatalf("failed to set HOME environment variable: %v", err)
		}

		defer func() {
			if err := os.Setenv("HOME", originalHome); err != nil {
				slog.Error("failed to restore HOME environment variable", "error", err)
			}
		}()

		// Create CLI without any pre-existing config by passing empty string
		// This should prevent automatic config creation
		cli := test.NewTestCLI(t, nil)

		// Before running init, verify no config exists
		configPath := filepath.Join(tempHomeDir, ".litebase", "config.yml")
		if _, err := os.Stat(configPath); !os.IsNotExist(err) {
			// If config exists, remove it to start clean
			if err := os.RemoveAll(filepath.Dir(configPath)); err != nil {
				t.Errorf("failed to remove existing config directory: %v", err)
			}
		}

		// Test with default path (should use ~/.litebase/config.yml)
		err := cli.Run("config", "init", "--cluster-id", "default-cluster", "--port", "9000")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check that the config file was created at the default path
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

		// Verify that an encryption key was generated
		if config.Server.Key == "" {
			t.Error("expected encryption key to be set")
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

		// Verify that an encryption key was generated
		if config.Server.Key == "" {
			t.Error("expected encryption key to be set")
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

		// Verify that the custom encryption key was used instead of generating one
		if config.Server.Key != "test-key" {
			t.Errorf("expected encryption key to be 'test-key', got %v", config.Server.Key)
		}
	})
}

func TestConfigInitPreventsOverwrite(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(t, nil)

		// Create a temporary directory for testing
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "existing-config.yml")

		// First, create an initial config
		err := cli.Run("config", "init", "--cluster-id", "initial-cluster", "--port", "8080", "--path", configPath)

		if err != nil {
			t.Fatalf("expected no error creating initial config, got %v", err)
		}

		// Verify the initial config was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Error("expected initial config file to be created")
		}

		// Now try to initialize again (should fail since init can only happen once)
		err = cli.Run("config", "init", "--cluster-id", "second-cluster", "--port", "9000", "--path", configPath)

		if err == nil {
			t.Error("expected error when trying to initialize again, but got none")
		}

		expectedErrorMsg := "configuration file already exists"
		if !strings.Contains(err.Error(), expectedErrorMsg) {
			t.Errorf("expected error message to contain '%s', got: %v", expectedErrorMsg, err.Error())
		}

		expectedErrorMsg2 := "can only be initialized once"
		if !strings.Contains(err.Error(), expectedErrorMsg2) {
			t.Errorf("expected error message to contain '%s', got: %v", expectedErrorMsg2, err.Error())
		}

		// Verify the original config is still intact
		configData, err := os.ReadFile(configPath)

		if err != nil {
			t.Fatalf("failed to read config file: %v", err)
		}

		var config config.CLIConfiguration

		if err := yaml.Unmarshal(configData, &config); err != nil {
			t.Fatalf("failed to unmarshal config: %v", err)
		}

		if config.Server.ClusterID != "initial-cluster" {
			t.Errorf("expected original cluster_id to be preserved as 'initial-cluster', got %v", config.Server.ClusterID)
		}

		if config.Server.Port != "8080" {
			t.Errorf("expected original port to be preserved as '8080', got %v", config.Server.Port)
		}
	})
}
