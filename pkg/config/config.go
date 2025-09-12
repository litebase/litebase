package config

import (
	"os"
)

const (
	EnvDevelopment = "development"
	EnvProduction  = "production"
	EnvTest        = "test"

	StorageModeLocal  = "local"
	StorageModeObject = "object"
)

type Config struct {
	ClusterId              string
	Debug                  bool
	DefaultBranchName      string
	EncryptionKey          string
	EncryptionKeyNext      string
	HostName               string
	Env                    string
	FakeObjectStorage      bool
	FileSystemDriver       string
	NodeAddressProvider    string
	PageSize               int64
	Port                   string
	RootPassword           string
	RootUsername           string
	StorageAccessKeyId     string
	StorageBucket          string
	StorageEndpoint        string
	StorageNetworkPath     string
	StorageObjectMode      string
	StoragePath            string
	StorageSecretAccessKey string
	StoragePort            string
	StorageRegion          string
	StorageTieredMode      string
	StorageTmpPath         string
}

// Get an environment variable or return a default value if not set.
func env(key string, defaultValue string) any {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	return defaultValue
}

// Create a new Config instance with values from environment variables or defaults.
func NewConfig() *Config {
	return &Config{
		ClusterId:              env("LITEBASE_CLUSTER_ID", "").(string),
		DefaultBranchName:      env("LITEBASE_DEFAULT_BRANCH_NAME", "main").(string),
		Debug:                  env("LITEBASE_DEBUG", "false") == "true",
		EncryptionKey:          env("LITEBASE_ENCRYPTION_KEY", "").(string),
		EncryptionKeyNext:      env("LITEBASE_ENCRYPTION_KEY_NEXT", "").(string),
		Env:                    env("LITEBASE_ENV", "production").(string),
		FakeObjectStorage:      env("LITEBASE_FAKE_OBJECT_STORAGE", "false") == "true",
		HostName:               env("LITEBASE_HOSTNAME", "localhost").(string),
		NodeAddressProvider:    env("LITEBASE_NODE_ADDRESS_PROVIDER", "").(string),
		PageSize:               4096,
		Port:                   env("LITEBASE_PORT", "8080").(string),
		RootPassword:           env("LITEBASE_ROOT_PASSWORD", "").(string),
		RootUsername:           env("LITEBASE_ROOT_USERNAME", "").(string),
		StorageAccessKeyId:     env("LITEBASE_STORAGE_ACCESS_KEY_ID", "").(string),
		StorageBucket:          env("LITEBASE_STORAGE_BUCKET", "").(string),
		StorageEndpoint:        env("LITEBASE_STORAGE_ENDPOINT", "").(string),
		StorageNetworkPath:     env("LITEBASE_STORAGE_NETWORK_PATH", "").(string),
		StorageRegion:          env("LITEBASE_STORAGE_REGION", "").(string),
		StorageObjectMode:      env("LITEBASE_STORAGE_OBJECT_MODE", "object").(string),
		StoragePath:            env("LITEBASE_DATA_PATH", "").(string),
		StorageSecretAccessKey: env("LITEBASE_STORAGE_SECRET_ACCESS_KEY", "").(string),
		StorageTieredMode:      env("LITEBASE_STORAGE_TIERED_MODE", env("LITEBASE_STORAGE_OBJECT_MODE", "object").(string)).(string),
		StorageTmpPath:         env("LITEBASE_STORAGE_TMP_PATH", "").(string),
	}
}
