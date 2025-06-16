package test

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

var envDataPath string

func setupTestEnv(t testing.TB) (string, error) {
	var err error

	log.SetFlags(log.LstdFlags | log.Lshortfile)
	rootDirectory := "./../../"

	if os.Getenv("LITEBASE_ROOT_DIRECTORY") != "" {
		rootDirectory = os.Getenv("LITEBASE_ROOT_DIRECTORY")
	}

	setTestEnvVariable(t)

	envPath := fmt.Sprintf("%s.env", rootDirectory)

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

	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(tmpPath, 0755)
	os.MkdirAll(dataPath+"/local", 0755)
	os.MkdirAll(dataPath+"/object", 0755)
	os.MkdirAll(dataPath+"/tiered", 0755)

	t.Setenv("LITEBASE_DOMAIN_NAME", "litebase.test")
	t.Setenv("LITEBASE_LOCAL_DATA_PATH", dataPath)
	t.Setenv("LITEBASE_NETWORK_STORAGE_PATH", networkStoragePath)
	t.Setenv("LITEBASE_TMP_PATH", tmpPath)

	var signature string
	if os.Getenv("LITEBASE_TEST_SIGNATURE") != "" {
		signature = os.Getenv("LITEBASE_TEST_SIGNATURE")
	} else {
		signature = CreateHash(64)
	}

	t.Setenv("LITEBASE_SIGNATURE", signature)

	slog.SetLogLoggerLevel(slog.LevelError)

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
			app.DatabaseManager.ShutdownResources()
			storage.Shutdown(app.Config)
			app.Cluster.Node().Shutdown()
		}

		for _, callback := range callbacks {
			callback()
		}

		// Remove the data path
		time.Sleep(100 * time.Millisecond) // Give some time for the app to shutdown
		os.RemoveAll(dataPath)
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
