package config

import (
	"crypto/sha256"
	"encoding/hex"

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
	DataPath               string
	DatabaseDirectory      string
	Debug                  bool
	DefaultBranchName      string
	EncryptionKey          string
	EncryptionKeyNext      string
	HostName               string
	Env                    string
	FakeObjectStorage      bool
	FileSystemDriver       string
	NetworkStoragePath     string
	NodeAddressProvider    string
	PageSize               int64
	Port                   string
	Region                 string
	RootPassword           string
	RootUsername           string
	RouterNodePort         string
	StorageAccessKeyId     string
	StorageBucket          string
	StorageEndpoint        string
	StorageObjectMode      string
	StorageSecretAccessKey string
	StoragePort            string
	StorageRegion          string
	StorageTieredMode      string
	TmpPath                string
}

func env(key string, defaultValue string) any {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	return defaultValue
}

func NewConfig() *Config {
	return &Config{
		ClusterId:              env("LITEBASE_CLUSTER_ID", "").(string),
		DataPath:               env("LITEBASE_LOCAL_DATA_PATH", "./data").(string),
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
		Region:                 env("LITEBASE_REGION", "").(string),
		NetworkStoragePath:     env("LITEBASE_NETWORK_STORAGE_PATH", "").(string),
		RouterNodePort:         env("LITEBASE_ROUTER_NODE_PORT", "8080").(string),
		RootPassword:           env("LITEBASE_ROOT_PASSWORD", "").(string),
		RootUsername:           env("LITEBASE_ROOT_USERNAME", "").(string),
		StorageAccessKeyId:     env("LITEBASE_STORAGE_ACCESS_KEY_ID", "").(string),
		StorageBucket:          env("LITEBASE_STORAGE_BUCKET", "").(string),
		StorageEndpoint:        env("LITEBASE_STORAGE_ENDPOINT", "").(string),
		StorageRegion:          env("LITEBASE_STORAGE_REGION", "").(string),
		StorageObjectMode:      env("LITEBASE_STORAGE_OBJECT_MODE", "object").(string),
		StorageSecretAccessKey: env("LITEBASE_STORAGE_SECRET_ACCESS_KEY", "").(string),
		StorageTieredMode:      env("LITEBASE_STORAGE_TIERED_MODE", env("LITEBASE_STORAGE_OBJECT_MODE", "object").(string)).(string),
		TmpPath:                env("LITEBASE_TMP_PATH", "").(string),
	}
}

// Generate a hash of the encryption key so that it is not stored in plain text.
func EncryptionKeyHash(encryptionKey string) string {
	hash := sha256.Sum256([]byte(encryptionKey))

	return hex.EncodeToString(hash[:])
}
