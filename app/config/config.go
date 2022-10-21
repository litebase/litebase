package config

import "os"

type Config struct {
	data map[string]string
}

var StaticConfig *Config

func NewConfig() *Config {
	StaticConfig = &Config{
		data: map[string]string{
			"app_host":                          os.Getenv("APP_HOST"),
			"branch_uuid":                       os.Getenv("LITEBASEDB_BRANCH_UUID"),
			"data_path":                         os.Getenv("LITEBASEDB_DATA_PATH"),
			"database_uuid":                     os.Getenv("LITEBASEDB_DB_UUID"),
			"encryption_key":                    os.Getenv("LITEBASEDB_ENCRYPTION_KEY"),
			"env":                               os.Getenv("APP_ENV"),
			"region":                            os.Getenv("LITEBASEDB_REGION"),
			"target_connection_time_in_seconds": os.Getenv("LITEBASEDB_TARGET_CONNECTION_TIME_IN_SECONDS"),
			"tmp_path":                          os.Getenv("LITEBASEDB_TMP_PATH"),
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
