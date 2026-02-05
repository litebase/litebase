package config

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strconv"
)

const (
	EnvDevelopment = "development"
	EnvProduction  = "production"
	EnvTest        = "test"

	StorageModeLocal   = "local"
	StorageModeNetwork = "network"
	StorageModeObject  = "object"
)

type Config struct {
	ClusterId                 string
	DataEncryptionKey         []byte
	DataEncryptionKeyHash     string
	DataEncryptionKeyNext     []byte
	DataEncryptionKeyNextHash string
	Debug                     bool
	DefaultBranchName         string
	EncryptionKey             string
	EncryptionKeyNext         string
	HostName                  string
	Env                       string
	FakeObjectStorage         bool
	FileSystemDriver          string
	MemoryLimit               int64
	NodeAddressProvider       string
	PageSize                  int64
	Port                      string
	PrivatePort               string
	RootPassword              string
	RootUsername              string
	StorageAccessKeyId        string
	StorageBucket             string
	StorageEndpoint           string
	StorageLocalPath          string
	StorageNetworkPath        string
	StorageObjectMode         string
	StorageSecretAccessKey    string
	StoragePort               string
	StorageRegion             string
	StorageTieredMode         string
	StorageTmpPath            string
}

// Get an environment variable or return a default value if not set.
func env(key string, defaultValue string) any {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	return defaultValue
}

// Parse memory limit from string (supports bytes as integer)
func parseMemoryLimit(value string) int64 {
	if value == "" {
		return 1024 * 1024 * 1024 // 1GB default
	}

	limit, err := strconv.ParseInt(value, 10, 64)

	if err != nil {
		return 1024 * 1024 * 1024 // 1GB default on error
	}

	return limit
}

// Create a new Config instance with values from environment variables or defaults.
func NewConfig() *Config {
	cfg := &Config{
		ClusterId:              env("LITEBASE_CLUSTER_ID", "").(string),
		DefaultBranchName:      env("LITEBASE_DEFAULT_BRANCH_NAME", "main").(string),
		Debug:                  env("LITEBASE_DEBUG", "false") == "true",
		EncryptionKey:          env("LITEBASE_ENCRYPTION_KEY", "").(string),
		EncryptionKeyNext:      env("LITEBASE_ENCRYPTION_KEY_NEXT", "").(string),
		Env:                    env("LITEBASE_ENV", "production").(string),
		FakeObjectStorage:      env("LITEBASE_FAKE_OBJECT_STORAGE", "false") == "true",
		HostName:               env("LITEBASE_HOSTNAME", "localhost").(string),
		MemoryLimit:            parseMemoryLimit(env("LITEBASE_MEMORY_LIMIT", "1073741824").(string)), // 1GB default
		NodeAddressProvider:    env("LITEBASE_NODE_ADDRESS_PROVIDER", "").(string),
		PageSize:               4096,
		Port:                   env("LITEBASE_PORT", "8080").(string),
		PrivatePort:            env("LITEBASE_PRIVATE_PORT", "0").(string), // 0 means auto-assign
		RootPassword:           env("LITEBASE_ROOT_PASSWORD", "").(string),
		RootUsername:           env("LITEBASE_ROOT_USERNAME", "").(string),
		StorageAccessKeyId:     env("LITEBASE_STORAGE_ACCESS_KEY_ID", "").(string),
		StorageBucket:          env("LITEBASE_STORAGE_BUCKET", "").(string),
		StorageEndpoint:        env("LITEBASE_STORAGE_ENDPOINT", "").(string),
		StorageLocalPath:       env("LITEBASE_STORAGE_LOCAL_PATH", "").(string),
		StorageNetworkPath:     env("LITEBASE_STORAGE_NETWORK_PATH", "").(string),
		StorageRegion:          env("LITEBASE_STORAGE_REGION", "").(string),
		StorageObjectMode:      env("LITEBASE_STORAGE_OBJECT_MODE", "object").(string),
		StorageSecretAccessKey: env("LITEBASE_STORAGE_SECRET_ACCESS_KEY", "").(string),
		StorageTieredMode:      env("LITEBASE_STORAGE_TIERED_MODE", env("LITEBASE_STORAGE_OBJECT_MODE", "object").(string)).(string),
		StorageTmpPath:         env("LITEBASE_STORAGE_TMP_PATH", os.TempDir()).(string),
	}

	// Load and hash DataEncryptionKey
	dataEncryptionKeyHex := env("LITEBASE_DATA_ENCRYPTION_KEY", "").(string)

	if dataEncryptionKeyHex != "" {
		dataKey, err := hex.DecodeString(dataEncryptionKeyHex)

		if err == nil && len(dataKey) == 32 {
			cfg.DataEncryptionKey = dataKey
			keyHash := sha256.Sum256(dataKey)
			cfg.DataEncryptionKeyHash = hex.EncodeToString(keyHash[:])
		}
	}

	// Load and hash DataEncryptionKeyNext
	dataEncryptionKeyNextHex := env("LITEBASE_DATA_ENCRYPTION_KEY_NEXT", "").(string)

	if dataEncryptionKeyNextHex != "" {
		dataKeyNext, err := hex.DecodeString(dataEncryptionKeyNextHex)

		if err == nil && len(dataKeyNext) == 32 {
			cfg.DataEncryptionKeyNext = dataKeyNext
			keyHash := sha256.Sum256(dataKeyNext)
			cfg.DataEncryptionKeyNextHash = hex.EncodeToString(keyHash[:])
		}
	}

	return cfg
}
