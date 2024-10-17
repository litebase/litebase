package test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/server"
	"litebase/server/cluster"
	"litebase/server/storage"
	"log"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
)

var envDataPath string

func setupTestEnv(t testing.TB) (string, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := godotenv.Load("./../../.env.test")

	if err != nil {
		log.Fatal(err)
	}

	if envDataPath == "" {
		envDataPath = os.Getenv("LITEBASE_LOCAL_DATA_PATH")
	}

	dataPath := fmt.Sprintf("%s/%s", envDataPath, CreateHash(64))
	tmpPath := fmt.Sprintf("%s/_tmp", dataPath)

	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(tmpPath, 0755)
	os.MkdirAll(dataPath+"/local", 0755)
	os.MkdirAll(dataPath+"/object", 0755)

	t.Setenv("LITEBASE_LOCAL_DATA_PATH", dataPath)
	t.Setenv("LITEBASE_TMP_PATH", tmpPath)
	t.Setenv("LITEBASE_SIGNATURE", CreateHash(64))

	return dataPath, err
}

func Setup(t testing.TB, callbacks ...func()) (*server.App, string) {
	dataPath, err := setupTestEnv(t)

	for _, callback := range callbacks {
		callback()
	}

	configInstance := config.NewConfig()

	app := server.NewApp(configInstance, server.NewServer(configInstance).ServeMux)

	if t != nil && err != nil {
		t.Fail()
	}

	app.Cluster.Node().Start()

	return app, dataPath
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
			app.Cluster.Node().Shutdown()
			app.DatabaseManager.ConnectionManager().Shutdown()
			app.DatabaseManager.ShutdownResources()
			storage.Shutdown(app.Config)
		}

		for _, callback := range callbacks {
			callback()
		}

		time.Sleep(100 * time.Millisecond)
		os.RemoveAll(dataPath)
	})
}

func Run(t testing.TB, callback func()) {
	cluster.SetAddressProvider(func() string {
		return "localhost"
	})

	// Setup the environment
	dataPath, err := SetupWithoutApp(t)

	if err != nil {
		t.Fail()
	}

	// Run the test
	callback()

	// Teardown the environment
	Teardown(t, dataPath, nil)
}

func RunWithApp(t testing.TB, callback func(*server.App)) {
	// Setup the environment
	app, dataPath := Setup(t)

	// Run the test
	callback(app)

	// Teardown the environment
	Teardown(t, dataPath, app)
}

func RunWithObjectStorage(t testing.TB, callback func(*server.App)) {
	t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
	bucketName := CreateHash(32)
	t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)

	// Setup the environment
	app, dataPath := Setup(t, func() {
	})

	// Run the test
	callback(app)

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
}
