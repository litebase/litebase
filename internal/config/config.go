package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"

	"os"
)

type Config struct {
	DataPath          string
	DatabaseDirectory string
	Debug             bool
	DefaultBranchName string
	Env               string
	FileSystemDriver  string
	PageSize          int64
	QueryNodePort     string
	Region            string
	RootPassword      string
	RouterNodePort    string
	Signature         string
	SignatureNext     string
	TmpPath           string
}

var ConfigInstance *Config = nil

func Init() {
	NewConfig()

	signature := Get().Signature

	storedSignature := storedSignature()

	if signature != "" && storedSignature == "" {
		StoreSignature(signature)
		return
	}

	if signature == "" && storedSignature != "" {
		ConfigInstance.Signature = storedSignature

		return
	}

	if signature != storedSignature {
		ConfigInstance.SignatureNext = storedSignature

		return
	}
	log.Println(signature)

	panic("The LITEBASE_SIGNATURE env variable is not set")
}

func env(key string, defaultValue string) interface{} {
	if os.Getenv(key) != "" {
		return os.Getenv(key)
	}

	return defaultValue
}

func NewConfig() *Config {
	ConfigInstance = &Config{
		DataPath:          os.Getenv("LITEBASEDB_DATA_PATH"),
		DefaultBranchName: env("LITEBASEDB_DEFAULT_BRANCH_NAME", "main").(string),
		Env:               env("LITEBASEDB_ENV", "production").(string),
		FileSystemDriver:  env("LITEBASEDB_FILESYSTEM_DRIVER", "local").(string),
		Debug:             env("LITEBASEDB_DEBUG", "false") == "true",
		PageSize:          65536,
		QueryNodePort:     env("LITEBASEDB_QUERY_NODE_PORT", "8080").(string),
		Region:            env("LITEBASEDB_REGION", "").(string),
		RouterNodePort:    env("LITEBASEDB_ROUTER_NODE_PORT", "8080").(string),
		RootPassword:      env("LITEBASEDB_ROOT_PASSWORD", "").(string),
		Signature:         env("LITEBASEDB_SIGNATURE", "").(string),
		TmpPath:           env("LITEBASEDB_TMP_PATH", "").(string),
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
