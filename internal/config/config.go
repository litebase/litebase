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
	QueryNodePort        string
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

	signature := Get().Signature

	storedSignature := storedSignature()

	if signature != "" && storedSignature == "" {
		StoreSignature(signature)
		return nil
	}

	if signature == "" && storedSignature != "" {
		ConfigInstance.Signature = storedSignature

		return nil
	}

	if signature != storedSignature {
		ConfigInstance.SignatureNext = storedSignature

		return nil
	}

	if signature != "" {
		return nil
	}

	return fmt.Errorf("the LITEBASE_SIGNATURE environment variable is not set")
}

func env(key string, defaultValue string) interface{} {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	return defaultValue
}

func NewConfig() *Config {
	ConfigInstance = &Config{
		DataPath:             env(os.Getenv("LITEBASE_DATA_PATH"), "./data").(string),
		DefaultBranchName:    env("LITEBASE_DEFAULT_BRANCH_NAME", "main").(string),
		Env:                  env("LITEBASE_ENV", "production").(string),
		FileSystemDriver:     env("LITEBASE_FILESYSTEM_DRIVER", "local").(string),
		Debug:                env("LITEBASE_DEBUG", "false") == "true",
		PageSize:             4096,
		QueryNodePort:        env("LITEBASE_QUERY_NODE_PORT", "8080").(string),
		Region:               env("LITEBASE_REGION", "").(string),
		RemoteStorageAddress: env("LITEBASE_REMOTE_STORAGE_ADDRESS", "").(string),
		RouterNodePort:       env("LITEBASE_ROUTER_NODE_PORT", "8080").(string),
		RootPassword:         env("LITEBASE_ROOT_PASSWORD", "").(string),
		RootUsername:         env("LITEBASE_ROOT_USERNAME", "root").(string),
		Signature:            env("LITEBASE_SIGNATURE", "").(string),
		StoragePort:          env("LITEBASE_STORAGE_PORT", "8080").(string),
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
	_, err := os.Stat(fmt.Sprintf("%s/.litebase/%s", Get().DataPath, SignatureHash(signature)))

	return err == nil
}

func StoreSignature(signature string) error {
	ConfigInstance.Signature = signature
	dataPath := Get().DataPath
	signaturePath := fmt.Sprintf("%s/.litebase/.signature", dataPath)

writeFile:

	err := os.WriteFile(signaturePath, []byte(signature), 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(fmt.Sprintf("%s/.litebase", dataPath), 0755)

			if err != nil {
				return err
			}

			goto writeFile

		}

		return err
	}

	return nil
}

// Generate a hash of the signature so that it is not stored in plain text.
func SignatureHash(signature string) string {
	hash := sha256.Sum256([]byte(signature))

	return hex.EncodeToString(hash[:])
}

func storedSignature() string {
	dataPath := Get().DataPath
	signaturePath := fmt.Sprintf("%s/.litebase/.signature", dataPath)

	storedSignature, err := os.ReadFile(signaturePath)

	if err != nil {
		return ""
	}

	return string(storedSignature)
}
