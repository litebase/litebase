package config

import (
	"fmt"
	"litebasedb/server/storage"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	AppHost                 string
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
	Port                    string
	Region                  string
	RootPassword            string
	Signature               string
	SignatureNext           string
	TmpPath                 string
	Url                     string
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
		AppHost:                 env("APP_HOST", "").(string),
		BranchUuid:              os.Getenv("LITEBASEDB_BRANCH_UUID"),
		DataPath:                os.Getenv("LITEBASEDB_DATA_PATH"),
		DatabaseUuid:            os.Getenv("LITEBASEDB_DB_UUID"),
		DefaultBranchName:       env("LITEBASEDB_DEFAULT_BRANCH_NAME", "main").(string),
		Env:                     env("APP_ENV", "production").(string),
		FileSystemDriver:        env("LITEBASEDB_FILESYSTEM_DRIVER", "local").(string),
		Debug:                   env("APP_DEBUG", "false") == "true",
		PageCompaction:          true,
		PageCompactionThreshold: 1000,
		PageCompactionInterval:  3000,
		PageSize:                4096,
		Port:                    env("LITEBASEDB_PORT", "8080").(string),
		Region:                  env("LITEBASEDB_REGION", "").(string),
		RootPassword:            env("LITEBASEDB_ROOT_PASSWORD", "").(string),
		Signature:               env("LITEBASEDB_SIGNATURE", "").(string),
		TmpPath:                 env("LITEBASEDB_TMP_PATH", "").(string),
		Url:                     env("APP_URL", "").(string),
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
