package test

import (
	"fmt"
	"litebase/server"
	"litebase/server/database"
	"litebase/server/node"
	"litebase/server/storage"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

var envDataPath string

func Setup(t testing.TB, callbacks ...func()) {
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

	for _, callback := range callbacks {
		callback()
	}

	// config.Get().SignatureNext = CreateHash(32)
	server.NewApp(server.NewServer())

	if t != nil && err != nil {
		t.Fail()
	}

	node.Node().Start()
}

func Teardown(callbacks ...func()) {
	database.ConnectionManager().Shutdown()
	database.ShutdownResources()
	node.Node().Shutdown()
	storage.Shutdown()

	os.RemoveAll(envDataPath)

	for _, callback := range callbacks {
		callback()
	}
}

func Run(t testing.TB, callback func()) {
	// Setup the environment
	Setup(t)
	// Run the test
	callback()
	// Teardown the environment
	Teardown()
}

func RunWithObjectStorage(t testing.TB, callback func()) {
	// Setup the environment
	Setup(t, func() {
		bucketName := CreateHash(32)
		t.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
		t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)
	})

	// Run the test
	callback()

	// Teardown the environment
	Teardown(func() {
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
