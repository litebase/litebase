package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"os"
)

const (
	EnvDevelopment = "development"
	EnvProduction  = "production"
	EnvTest        = "test"

	NodeTypeQuery   = "query"
	NodeTypeStorage = "storage"

	StorageModeDistributed = "distributed"
	StorageModeLocal       = "local"
	StorageModeObject      = "object"
)

type Config struct {
	ClusterId              string
	DataPath               string
	DatabaseDirectory      string
	Debug                  bool
	DefaultBranchName      string
	Env                    string
	FileSystemDriver       string
	NodeType               string
	PageSize               int64
	Port                   string
	Region                 string
	RemoteStorageAddress   string
	RootPassword           string
	RootUsername           string
	RouterNodePort         string
	Signature              string
	SignatureNext          string
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

func env(key string, defaultValue string) interface{} {
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
		Env:                    env("LITEBASE_ENV", "production").(string),
		Debug:                  env("LITEBASE_DEBUG", "false") == "true",
		NodeType:               env("LITEBASE_NODE_TYPE", NodeTypeQuery).(string),
		PageSize:               4096,
		Port:                   env("LITEBASE_PORT", "8080").(string),
		Region:                 env("LITEBASE_REGION", "").(string),
		RemoteStorageAddress:   env("LITEBASE_REMOTE_STORAGE_ADDRESS", "").(string),
		RouterNodePort:         env("LITEBASE_ROUTER_NODE_PORT", "8080").(string),
		RootPassword:           env("LITEBASE_ROOT_PASSWORD", "").(string),
		RootUsername:           env("LITEBASE_ROOT_USERNAME", "root").(string),
		Signature:              env("LITEBASE_SIGNATURE", "").(string),
		StorageAccessKeyId:     env("LITEBASE_STORAGE_ACCESS_KEY_ID", "").(string),
		StorageBucket:          env("LITEBASE_STORAGE_BUCKET", "").(string),
		StorageEndpoint:        env("LITEBASE_STORAGE_ENDPOINT", "").(string),
		StorageRegion:          env("LITEBASE_STORAGE_REGION", "").(string),
		StorageObjectMode:      env("LITEBASE_STORAGE_OBJECT_MODE", "local").(string),
		StorageSecretAccessKey: env("LITEBASE_STORAGE_SECRET_ACCESS_KEY", "").(string),
		StorageTieredMode:      env("LITEBASE_STORAGE_TIERED_MODE", "local").(string),
		TmpPath:                env("LITEBASE_TMP_PATH", "").(string),
	}
}

// Check if the signature directory exists
func HasSignature(config *Config, signature string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s", config.DataPath, SignatureHash(signature)))

	return err == nil
}

// Generate a hash of the signature so that it is not stored in plain text.
func SignatureHash(signature string) string {
	hash := sha256.Sum256([]byte(signature))

	return hex.EncodeToString(hash[:])
}
