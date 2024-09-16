package test

import (
	"context"
	"fmt"
	"litebase/server"
	"litebase/server/database"
	"litebase/server/node"
	"litebase/server/storage"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
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
}

func Teardown(callbacks ...func()) {
	database.ConnectionManager().Shutdown()
	node.Node().Shutdown()
	storage.Shutdown()

	err := os.RemoveAll(os.Getenv("LITEBASE_LOCAL_DATA_PATH"))

	if err != nil {
		log.Fatal(err)
	}

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
		t.Setenv("LITEBASE_STORAGE_MODE", "object")
		t.Setenv("LITEBASE_STORAGE_BUCKET", bucketName)

		if host := os.Getenv("LITEBASE_STORAGE_HOST"); host == "" {
			log.Fatal("LITEBASE_STORAGE_HOST is not set")
		}

		mc, err := mclient.New(os.Getenv("LITEBASE_STORAGE_HOST"), &mclient.Options{
			Creds:  credentials.NewStaticV4(os.Getenv("MINIO_ROOT_USER"), os.Getenv("MINIO_ROOT_PASSWORD"), ""),
			Secure: false,
		})

		if err != nil {
			log.Fatal("Error creating Minio client", err)
		}

		err = mc.MakeBucket(
			context.Background(),
			os.Getenv("LITEBASE_STORAGE_BUCKET"),
			mclient.MakeBucketOptions{},
		)

		if err != nil {
			log.Fatal("Error creating bucket", err)
		}
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
