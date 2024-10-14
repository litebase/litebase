package test

import (
	"fmt"
	"litebase/server"
	"litebase/server/storage"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

var envDataPath string

func setupTestEvn(t testing.TB) error {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := godotenv.Load("./../../.env.test")

	if err != nil {
		log.Fatal(err)
	}

	if envDataPath == "" {
		envDataPath = os.Getenv("LITEBASE_LOCAL_DATA_PATH")
	}

	// Initialize the storage files systems
	storage.InitFS()

	dataPath := fmt.Sprintf("%s/%s", envDataPath, CreateHash(64))
	tmpPath := fmt.Sprintf("%s/_tmp", dataPath)

	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(tmpPath, 0755)
	os.MkdirAll(dataPath+"/local", 0755)
	os.MkdirAll(dataPath+"/object", 0755)

	t.Setenv("LITEBASE_LOCAL_DATA_PATH", dataPath)
	t.Setenv("LITEBASE_TMP_PATH", tmpPath)
	t.Setenv("LITEBASE_SIGNATURE", CreateHash(64))

	return err
}

func Setup(t testing.TB, callbacks ...func()) *server.App {
	err := setupTestEvn(t)

	for _, callback := range callbacks {
		callback()
	}

	app := server.NewApp(server.NewServer().ServeMux)

	if t != nil && err != nil {
		t.Fail()
	}

	app.Cluster.Node().Start()

	return app
}

func SetupWithoutApp(t testing.TB, callbacks ...func()) error {
	err := setupTestEvn(t)

	for _, callback := range callbacks {
		callback()
	}

	return err
}

func Teardown(app *server.App, callbacks ...func()) {
	if app != nil {
		app.DatabaseManager.ConnectionManager().Shutdown()
		app.DatabaseManager.ShutdownResources()
		app.Cluster.Node().Shutdown()
	}

	storage.Shutdown()

	os.RemoveAll(envDataPath)

	for _, callback := range callbacks {
		callback()
	}
}

func Run(t testing.TB, callback func()) {
	// Setup the environment
	SetupWithoutApp(t)

	// Run the test
	callback()

	// Teardown the environment
	Teardown(nil)
}

func RunWithApp(t testing.TB, callback func(*server.App)) {
	// Setup the environment
	app := Setup(t)
	// Run the test
	callback(app)
	// Teardown the environment
	Teardown(app)
}

func RunWithObjectStorage(t testing.TB, callback func(*server.App)) {
	// Setup the environment
	app := Setup(t, func() {
		bucketName := CreateHash(32)
		t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
		t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)
	})

	// Run the test
	callback(app)

	// Teardown the environment
	Teardown(app, func() {
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
