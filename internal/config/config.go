package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"os"
)

type Config struct {
	DataPath             string
	DatabaseDirectory    string
	Debug                bool
	DefaultBranchName    string
	Env                  string
	FileSystemDriver     string
	PageSize             int64
	Port                 string
	Region               string
	RemoteStorageAddress string
	RootPassword         string
	RootUsername         string
	RouterNodePort       string
	Signature            string
	SignatureNext        string
	StoragePort          string
	TmpPath              string
}

var ConfigInstance *Config = nil

func Init() error {
	NewConfig()

	return nil
}

func env(key string, defaultValue string) interface{} {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	return defaultValue
}

func NewConfig() *Config {
	ConfigInstance = &Config{
		DataPath:             env("LITEBASE_LOCAL_DATA_PATH", "./data").(string),
		DefaultBranchName:    env("LITEBASE_DEFAULT_BRANCH_NAME", "main").(string),
		Env:                  env("LITEBASE_ENV", "production").(string),
		FileSystemDriver:     env("LITEBASE_FILESYSTEM_DRIVER", "local").(string),
		Debug:                env("LITEBASE_DEBUG", "false") == "true",
		PageSize:             4096,
		Port:                 env("LITEBASE_PORT", "8080").(string),
		Region:               env("LITEBASE_REGION", "").(string),
		RemoteStorageAddress: env("LITEBASE_REMOTE_STORAGE_ADDRESS", "").(string),
		RouterNodePort:       env("LITEBASE_ROUTER_NODE_PORT", "8080").(string),
		RootPassword:         env("LITEBASE_ROOT_PASSWORD", "").(string),
		RootUsername:         env("LITEBASE_ROOT_USERNAME", "root").(string),
		Signature:            env("LITEBASE_SIGNATURE", "").(string),
		TmpPath:              env("LITEBASE_TMP_PATH", "").(string),
	}

	return ConfigInstance
}

func Get() *Config {
	if ConfigInstance == nil {
		NewConfig()
	}

	return ConfigInstance
}

// Check if the signature directory exists
func HasSignature(signature string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/%s", Get().DataPath, SignatureHash(signature)))

	return err == nil
}

// Generate a hash of the signature so that it is not stored in plain text.
func SignatureHash(signature string) string {
	hash := sha256.Sum256([]byte(signature))

	return hex.EncodeToString(hash[:])
}
