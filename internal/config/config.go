package config

import (
	"fmt"
	"litebasedb/server/storage"

	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	BranchUuid              string
	DatabaseUuid            string
	DataPath                string
	DatabaseDirectory       string
	DatabaseName            string
	Debug                   bool
	DefaultBranchName       string
	Env                     string
	FileSystemDriver        string
	PageCompaction          bool
	PageCompactionThreshold int64
	PageCompactionInterval  int64
	PageSize                int64
	QueryNodePort           string
	Region                  string
	RootPassword            string
	RouterNodePort          string
	Signature               string
	SignatureNext           string
	TmpPath                 string
}

var ConfigInstance *Config = nil

func Init() {
	NewConfig()

	signature := Get().Signature
	storedSignature := storedSignature()

	if storedSignature == "" {
		return
	}

	if signature != storedSignature {
		ConfigInstance.Signature = storedSignature
	} else {
		storage.FS().Remove(fmt.Sprintf("%s/.litebasedb/signature", Get().DataPath))
	}
}

func env(key string, defaultValue string) interface{} {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	// if defaultValue == "" {
	// 	panic(fmt.Sprintf("Environment variable %s is not set", key))
	// }

	return defaultValue
}

func NewConfig() *Config {
	godotenv.Load()

	ConfigInstance = &Config{
		DataPath:                os.Getenv("LITEBASEDB_DATA_PATH"),
		DefaultBranchName:       env("LITEBASEDB_DEFAULT_BRANCH_NAME", "main").(string),
		Env:                     env("LITEBASEDB_ENV", "production").(string),
		FileSystemDriver:        env("LITEBASEDB_FILESYSTEM_DRIVER", "local").(string),
		Debug:                   env("LITEBASEDB_DEBUG", "false") == "true",
		PageCompaction:          true,
		PageCompactionThreshold: 1000,
		PageCompactionInterval:  3000,
		PageSize:                65536, //4096,
		QueryNodePort:           env("LITEBASEDB_QUERY_NODE_PORT", "8080").(string),
		Region:                  env("LITEBASEDB_REGION", "").(string),
		RouterNodePort:          env("LITEBASEDB_ROUTER_NODE_PORT", "8080").(string),
		RootPassword:            env("LITEBASEDB_ROOT_PASSWORD", "").(string),
		Signature:               env("LITEBASEDB_SIGNATURE", "").(string),
		TmpPath:                 env("LITEBASEDB_TMP_PATH", "").(string),
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
	_, err := storage.FS().Stat(fmt.Sprintf("%s/.litebasedb/%s", Get().DataPath, signature))

	return err == nil
}

func StoreSignature(signature string) error {
	ConfigInstance.Signature = signature
	dataPath := Get().DataPath
	signaturePath := fmt.Sprintf("%s/.litebasedb/signature", dataPath)

	return storage.FS().WriteFile(signaturePath, []byte(signature), 0644)
}

func storedSignature() string {
	dataPath := Get().DataPath
	signaturePath := fmt.Sprintf("%s/.litebasedb/signature", dataPath)

	storedSignature, err := storage.FS().ReadFile(signaturePath)

	if err != nil {
		return ""
	}

	return string(storedSignature)
}
