package config

import (
	"fmt"
	"os"
)

type Config struct {
	data map[string]string
}

var StaticConfig *Config

func Init() {
	NewConfig()

	signature := Get("signature")
	storedSignature := storedSignature()

	if storedSignature == "" {
		return
	}

	if signature != storedSignature {
		Set("signature", storedSignature)
	} else {
		os.Remove(fmt.Sprintf("%s/.litebasedb/signature", Get("data_path")))
	}
}

func NewConfig() *Config {
	StaticConfig = &Config{
		data: map[string]string{
			"app_host":                          os.Getenv("APP_HOST"),
			"branch_uuid":                       os.Getenv("LITEBASEDB_BRANCH_UUID"),
			"data_path":                         os.Getenv("LITEBASEDB_DATA_PATH"),
			"database_uuid":                     os.Getenv("LITEBASEDB_DB_UUID"),
			"env":                               os.Getenv("APP_ENV"),
			"port":                              os.Getenv("LITEBASEDB_PORT"),
			"region":                            os.Getenv("LITEBASEDB_REGION"),
			"root_password":                     os.Getenv("LITEBASEDB_ROOT_PASSWORD"),
			"signature":                         os.Getenv("LITEBASEDB_SIGNATURE"),
			"target_connection_time_in_seconds": os.Getenv("LITEBASEDB_TARGET_CONNECTION_TIME_IN_SECONDS"),
			"tmp_path":                          os.Getenv("LITEBASEDB_TMP_PATH"),
			"url":                               os.Getenv("APP_URL"),
		},
	}

	return StaticConfig
}

func Get(key string) string {
	if StaticConfig == nil {
		NewConfig()
	}

	if value, ok := StaticConfig.data[key]; ok {
		return value
	}

	return ""
}

// Check if the signature directory exists
func HasSignature(signature string) bool {
	_, err := os.Stat(fmt.Sprintf("%s/.litebasedb/%s", Get("data_path"), signature))

	return err == nil
}

func Set(key string, value string) {
	if StaticConfig == nil {
		NewConfig()
	}

	StaticConfig.data[key] = value
}

func StoreSignature(signature string) error {
	Set("signature", signature)
	dataPath := Get("data_path")
	signaturePath := fmt.Sprintf("%s/.litebasedb/signature", dataPath)

	return os.WriteFile(signaturePath, []byte(signature), 0644)
}

func storedSignature() string {
	dataPath := Get("data_path")
	signaturePath := fmt.Sprintf("%s/.litebasedb/signature", dataPath)

	storedSignature, err := os.ReadFile(signaturePath)

	if err != nil {
		return ""
	}

	return string(storedSignature)
}

func Swap(key string, value string, callback func() interface{}) interface{} {
	originalValue := Get(key)

	Set(key, value)

	result := callback()
	Set(key, originalValue)

	return result
}
