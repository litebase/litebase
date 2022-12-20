package config

import "os"

type Config struct {
	data map[string]string
}

var StaticConfig *Config

func NewConfig() *Config {
	StaticConfig = &Config{
		data: map[string]string{
			"app_host":  os.Getenv("APP_HOST"),
			"data_path": os.Getenv("LITEBASEDB_DATA_PATH"),
			"env":       os.Getenv("APP_ENV"),
			"port":      os.Getenv("LITEBASEDB_PORT"),
			"region":    os.Getenv("LITEBASEDB_REGION"),
			"signature": os.Getenv("LITEBASEDB_SIGNATURE"),
			"url":       os.Getenv("APP_URL"),
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

func Set(key string, value string) {
	if StaticConfig == nil {
		NewConfig()
	}

	StaticConfig.data[key] = value
}

func Swap(key string, value string, callback func() interface{}) interface{} {
	originalValue := Get(key)

	Set(key, value)

	result := callback()
	Set(key, originalValue)

	return result
}
