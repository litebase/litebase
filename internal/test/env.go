package test

import "testing"

func setTestEnvVariable(t testing.TB) {
	t.Setenv("LITEBASE_CLUSTER_ID", "cluster-1")
	t.Setenv("LITEBASE_DEBUG", "true")
	t.Setenv("LITEBASE_ENV", "test")
	t.Setenv("LITEBASE_FILESYSTEM_DRIVER", "local")
	t.Setenv("LITEBASE_LOCAL_DATA_PATH", ".test")
	t.Setenv("LITEBASE_PORT", "8080")
	t.Setenv("LITEBASE_REGION", "us-east-1")
	t.Setenv("LITEBASE_SHARED_PATH", "/.test/shared")
	t.Setenv("LITEBASE_ROOT_PASSWORD", "password")
	t.Setenv("LITEBASE_ROOT_USERNAME", "root")
	t.Setenv("LITEBASE_ROUTER_NODE_PORT", "8080")
	t.Setenv("LITESBASE_ROUTER_NODE_PATH", "/.test/nodes/router")
	t.Setenv("LITEBASE_SIGNATURE", "a1f25fb235eccdb3c007356da75d84f921c573c4c65e94a78f1a0c3f834a275a")
	t.Setenv("LITEBASE_STORAGE_ENDPOINT", "http://s3.test:9000")
	t.Setenv("LITEBASE_STORAGE_BUCKET", "litebase-test")
	t.Setenv("LITEBASE_STORAGE_ACCESS_KEY_ID", "litebase_test")
	t.Setenv("LITEBASE_STORAGE_SECRET_ACCESS_KEY", "litebase_test")
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "local")
	t.Setenv("LITEBASE_STORAGE_REGION", "auto")
	t.Setenv("LITEBASE_STORAGE_TIERED_MODE", "local")
	t.Setenv("LITEBASE_TMP_PATH", "/.test/_tmp")
}
