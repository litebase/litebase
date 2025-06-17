package test

import (
	"os"
	"testing"
)

func setTestEnvVariable(t testing.TB) {
	envVars := map[string]string{
		"LITEBASE_CLUSTER_ID":                "cluster-1",
		"LITEBASE_DEBUG":                     "true",
		"LITEBASE_ENV":                       "test",
		"LITEBASE_HOSTNAME":                  "localhost",
		"LITEBASE_FILESYSTEM_DRIVER":         "local",
		"LITEBASE_LOCAL_DATA_PATH":           ".test",
		"LITEBASE_PORT":                      "8080",
		"LITEBASE_REGION":                    "us-east-1",
		"LITEBASE_SHARED_PATH":               "/.test/shared",
		"LITEBASE_ROOT_PASSWORD":             "password",
		"LITEBASE_ROOT_USERNAME":             "root",
		"LITEBASE_ROUTER_NODE_PORT":          "8080",
		"LITEBASE_ROUTER_NODE_PATH":          "/.test/nodes/router",
		"LITEBASE_SIGNATURE":                 "a1f25fb235eccdb3c007356da75d84f921c573c4c65e94a78f1a0c3f834a275a",
		"LITEBASE_STORAGE_ENDPOINT":          "http://s3.test:9000",
		"LITEBASE_STORAGE_BUCKET":            "litebase-test",
		"LITEBASE_STORAGE_ACCESS_KEY_ID":     "litebase_test",
		"LITEBASE_STORAGE_SECRET_ACCESS_KEY": "litebase_test",
		"LITEBASE_STORAGE_OBJECT_MODE":       "local",
		"LITEBASE_STORAGE_REGION":            "auto",
		"LITEBASE_STORAGE_TIERED_MODE":       "local",
		"LITEBASE_TMP_PATH":                  "/.test/_tmp",
	}

	for key, value := range envVars {
		if os.Getenv(key) == "" {
			t.Setenv(key, value)
		}
	}
}
