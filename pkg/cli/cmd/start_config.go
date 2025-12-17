package cmd

import (
	"fmt"
	"os"
)

type StartConfig struct {
	ClusterID              string `yaml:"cluster_id"`
	ConfigPath             string `yaml:"config_path"`
	Debug                  bool   `yaml:"debug,omitempty"`
	Key                    string `yaml:"key"`
	Port                   string `yaml:"port"`
	StorageAccessKeyId     string `yaml:"storage_access_key_id,omitempty"`
	StorageBucket          string `yaml:"storage_bucket,omitempty"`
	StorageEndpoint        string `yaml:"storage_endpoint,omitempty"`
	StorageLocalPath       string `yaml:"storage_local_path"`
	StorageNetworkPath     string `yaml:"storage_network_path"`
	StorageObjectMode      string `yaml:"storage_object_mode,omitempty"`
	StorageRegion          string `yaml:"storage_region,omitempty"`
	StorageSecretAccessKey string `yaml:"storage_secret_access_key,omitempty"`
	StorageTmpPath         string `yaml:"storage_tmp_path"`
	TLSCertPath            string `yaml:"tls_cert_path"`
	TLSKeyPath             string `yaml:"tls_key_path"`
}

type StartServerConfig struct {
}

// Load the start config values into environment variables.
func (c *StartConfig) Load() error {
	if c.Debug {
		if err := os.Setenv("LITEBASE_DEBUG", "true"); err != nil {
			return fmt.Errorf("failed to set LITEBASE_DEBUG environment variable: %w", err)
		}
	}

	if c.Port != "" {
		if err := os.Setenv("LITEBASE_PORT", c.Port); err != nil {
			return fmt.Errorf("failed to set LITEBASE_PORT environment variable: %w", err)
		}
	}

	if c.StorageLocalPath != "" {
		if err := os.Setenv("LITEBASE_STORAGE_LOCAL_PATH", c.StorageLocalPath); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_LOCAL_PATH environment variable: %w", err)
		}
	}

	if c.StorageNetworkPath != "" {
		if err := os.Setenv("LITEBASE_STORAGE_NETWORK_PATH", c.StorageNetworkPath); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_NETWORK_PATH environment variable: %w", err)
		}
	}

	if c.StorageTmpPath != "" {
		if err := os.Setenv("LITEBASE_STORAGE_TMP_PATH", c.StorageTmpPath); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_TMP_PATH environment variable: %w", err)
		}
	}

	if c.TLSCertPath != "" {
		if err := os.Setenv("LITEBASE_TLS_CERT_PATH", c.TLSCertPath); err != nil {
			return fmt.Errorf("failed to set LITEBASE_TLS_CERT_PATH environment variable: %w", err)
		}
	}

	if c.TLSKeyPath != "" {
		if err := os.Setenv("LITEBASE_TLS_KEY_PATH", c.TLSKeyPath); err != nil {
			return fmt.Errorf("failed to set LITEBASE_TLS_KEY_PATH environment variable: %w", err)
		}
	}

	if c.Key != "" {
		if err := os.Setenv("LITEBASE_ENCRYPTION_KEY", c.Key); err != nil {
			return fmt.Errorf("failed to set LITEBASE_ENCRYPTION_KEY environment variable: %w", err)
		}
	}

	if c.ClusterID != "" {
		if err := os.Setenv("LITEBASE_CLUSTER_ID", c.ClusterID); err != nil {
			return fmt.Errorf("failed to set LITEBASE_CLUSTER_ID environment variable: %w", err)
		}
	}

	if c.StorageObjectMode != "" {
		if err := os.Setenv("LITEBASE_STORAGE_OBJECT_MODE", c.StorageObjectMode); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_OBJECT_MODE environment variable: %w", err)
		}
	}

	if c.StorageBucket != "" {
		if err := os.Setenv("LITEBASE_STORAGE_BUCKET", c.StorageBucket); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_BUCKET environment variable: %w", err)
		}
	}

	if c.StorageEndpoint != "" {
		if err := os.Setenv("LITEBASE_STORAGE_ENDPOINT", c.StorageEndpoint); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_ENDPOINT environment variable: %w", err)
		}
	}

	if c.StorageRegion != "" {
		if err := os.Setenv("LITEBASE_STORAGE_REGION", c.StorageRegion); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_REGION environment variable: %w", err)
		}
	}

	if c.StorageAccessKeyId != "" {
		if err := os.Setenv("LITEBASE_STORAGE_ACCESS_KEY_ID", c.StorageAccessKeyId); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_ACCESS_KEY_ID environment variable: %w", err)
		}
	}

	if c.StorageSecretAccessKey != "" {
		if err := os.Setenv("LITEBASE_STORAGE_SECRET_ACCESS_KEY", c.StorageSecretAccessKey); err != nil {
			return fmt.Errorf("failed to set LITEBASE_STORAGE_SECRET_ACCESS_KEY environment variable: %w", err)
		}
	}

	return nil
}
