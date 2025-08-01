package test

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

var envDataPath string

// findProjectRoot searches for the project root by looking for go.mod file
func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}

	// Walk up the directory tree looking for go.mod
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			// We've reached the root of the filesystem
			break
		}
		dir = parent
	}

	return "", fmt.Errorf("could not find go.mod file in any parent directory")
}

func setupDirectories(dataPath string) error {
	directories := []string{
		dataPath,
		fmt.Sprintf("%s/_tmp", dataPath),
		fmt.Sprintf("%s/tiered", dataPath),
		fmt.Sprintf("%s/object", dataPath),
		fmt.Sprintf("%s/local", dataPath),
	}

	for _, dir := range directories {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

func setupTestEnv(t testing.TB) (string, error) {
	var err error

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var rootDirectory string
	if os.Getenv("LITEBASE_ROOT_DIRECTORY") != "" {
		rootDirectory = os.Getenv("LITEBASE_ROOT_DIRECTORY")
	} else {
		rootDirectory, err = findProjectRoot()

		if err != nil {
			return "", fmt.Errorf("failed to find project root: %w", err)
		}
	}

	setTestEnvVariable(t)

	envPath := fmt.Sprintf("%s.env.test", rootDirectory)

	if _, err := os.Stat(envPath); err == nil {
		err := godotenv.Load(envPath)

		if err != nil {
			log.Fatal(err)
		}
	}

	if envDataPath == "" {
		envDataPath = fmt.Sprintf("%s%s", rootDirectory, os.Getenv("LITEBASE_LOCAL_DATA_PATH"))
	}

	var dataPath string

	if os.Getenv("LITEBASE_TEST_DATA_PATH") != "" {
		dataPath = os.Getenv("LITEBASE_TEST_DATA_PATH")
	} else {
		dataPath = fmt.Sprintf("%s/%s", envDataPath, CreateHash(64))
	}

	networkStoragePath := fmt.Sprintf("%s/_network_storage", dataPath)
	tmpPath := fmt.Sprintf("%s/_tmp", dataPath)

	if err := setupDirectories(dataPath); err != nil {
		t.Fatalf("failed to setup directories: %v", err)
	}

	t.Setenv("LITEBASE_LOCAL_DATA_PATH", dataPath)
	t.Setenv("LITEBASE_NETWORK_STORAGE_PATH", networkStoragePath)
	t.Setenv("LITEBASE_TMP_PATH", tmpPath)

	var encryptionKey string
	if os.Getenv("LITEBASE_TEST_ENCRYPTION_KEY") != "" {
		encryptionKey = os.Getenv("LITEBASE_TEST_ENCRYPTION_KEY")
	} else {
		encryptionKey = CreateHash(64)
	}

	t.Setenv("LITEBASE_ENCRYPTION_KEY", encryptionKey)

	if os.Getenv("LITEBASE_TEST_DEBUG_LEVEL") == "debug" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	} else {
		slog.SetLogLoggerLevel(slog.LevelError)
	}

	return dataPath, err
}

func Setup(t testing.TB, callbacks ...func()) (*server.App, string) {
	dataPath, err := setupTestEnv(t)

	for _, callback := range callbacks {
		callback()
	}

	s := NewTestServer(t)

	if t != nil && err != nil {
		t.Fail()
	}

	return s.App, dataPath
}

func SetupWithoutApp(t testing.TB, callbacks ...func()) (string, error) {
	dataPath, err := setupTestEnv(t)

	for _, callback := range callbacks {
		callback()
	}

	return dataPath, err
}

func Teardown(t testing.TB, dataPath string, app *server.App, callbacks ...func()) {
	t.Cleanup(func() {
		if app != nil {
			app.DatabaseManager.ConnectionManager().Shutdown()
			err := app.DatabaseManager.ShutdownResources()

			if err != nil {
				log.Printf("failed to shutdown database manager resources: %v", err)
			}

			err = app.Cluster.Node().Shutdown()

			if err != nil {
				log.Printf("failed to shutdown cluster node: %v", err)
			}

			storage.Shutdown(app.Config)
		}

		for _, callback := range callbacks {
			callback()
		}

		// Remove the data path
		time.Sleep(100 * time.Millisecond) // Give some time for the app to shutdown

		err := os.RemoveAll(dataPath)

		if err != nil {
			log.Printf("failed to remove data path %s: %v", dataPath, err)
		}
	})
}

func Run(t testing.TB, callback func()) {
	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Setup the environment
	dataPath, err := SetupWithoutApp(t)

	if err != nil {
		t.Fail()
	}

	// Teardown the environment
	Teardown(t, dataPath, nil)

	// Run the test
	callback()
}

func RunWithTearDown(t testing.TB, callback func(), callbacks ...func()) {
	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Setup the environment
	dataPath, err := SetupWithoutApp(t)

	if err != nil {
		t.Fail()
	}

	// Teardown the environment
	Teardown(t, dataPath, nil, callbacks...)

	// Run the test
	callback()
}

func RunWithApp(t testing.TB, callback func(*server.App)) {
	// Setup the environment
	app, dataPath := Setup(t)

	// Teardown the environment
	Teardown(t, dataPath, app)

	// Run the test
	callback(app)
}

func RunWithObjectStorage(t testing.TB, callback func(*server.App)) {
	t.Setenv("LITEBASE_FAKE_OBJECT_STORAGE", "true")
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
	t.Setenv("LITEBASE_STORAGE_BUCKET", CreateHash(32))
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", config.StorageModeObject)
	t.Setenv("LITEBASE_STORAGE_TIERED_MODE", config.StorageModeObject)

	// Setup the environment
	app, dataPath := Setup(t, func() {
	})

	// Teardown the environment
	Teardown(t, dataPath, app, func() {
		// Remove the bucket
		err := os.RemoveAll(
			fmt.Sprintf("%s/_object_storage/%s",
				os.Getenv("LITEBASE_LOCAL_DATA_PATH"),
				os.Getenv("LITEBASE_STORAGE_BUCKET")),
		)

		if err != nil {
			log.Fatal(err)
		}
	})

	// Run the test
	callback(app)
}

// RunWithoutCleanup runs a test with app setup but without cleanup.
// This is useful for forked test processes that share resources with other processes.
// The parent process should handle cleanup, not the individual forked processes.
func RunWithoutCleanup(t testing.TB, callback func(*server.App)) {
	cluster.SetAddressProvider(func() string {
		return "127.0.0.1"
	})

	// Setup the environment without cleanup
	_, err := setupTestEnv(t)

	if err != nil {
		t.Fail()
	}

	// Create the app without registering cleanup
	s := NewTestServer(t)

	if t != nil && err != nil {
		t.Fail()
	}

	// Run the test callback
	callback(s.App)
}
