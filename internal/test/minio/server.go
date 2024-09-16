package minio

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio/cmd"
)

var objectStorageAddress string
var minioShutdown func() error

func SetupObjectStorage(m *testing.M, callback func()) {
	err := godotenv.Load("./../../.env.test")

	if err != nil {
		log.Fatal(err)
	}

	err = os.MkdirAll(fmt.Sprintf("%s/_object_storage", os.Getenv("LITEBASE_LOCAL_DATA_PATH")), 0755)

	if err != nil {
		log.Fatal(err)
	}

	objectStorageAddress, minioShutdown, err = StartMinioServer(fmt.Sprintf("%s/_object_storage", os.Getenv("LITEBASE_LOCAL_DATA_PATH")))

	if err != nil {
		log.Fatal(err)
	}

	os.Setenv("LITEBASE_STORAGE_HOST", objectStorageAddress)
	os.Setenv("LITEBASE_STORAGE_ENDPOINT", fmt.Sprintf("http://%s", objectStorageAddress))

	// Run the test
	callback()

	// Teardown the environment
	StopMinioServer(minioShutdown)
}

func StartMinioServer(directory string) (string, func() error, error) {
	l, err := net.Listen("tcp", "localhost:0")

	if err != nil {
		return "", nil, err
	}

	addr := l.Addr().String()

	err = l.Close()

	if err != nil {
		return "", nil, err
	}

	accessKeyID := os.Getenv("MINIO_ROOT_USER")
	secretAccessKey := os.Getenv("MINIO_ROOT_PASSWORD")

	madm, err := madmin.New(addr, accessKeyID, secretAccessKey, false)

	if err != nil {
		log.Println("Error creating Minio admin client", err)
		return "", nil, err
	}

	go func() {
		minio.Main([]string{
			"minio",
			"server",
			"--quiet",
			"--address",
			addr,
			directory,
		})
	}()

	ready := make(chan bool)

	go func() {
		for {
			_, err := madm.ServerInfo(context.Background())

			if err == nil {
				ready <- true
				break
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()

	<-ready

	return addr, func() error {
		err := madm.ServiceStop(context.Background())

		if err != nil {
			return err
		}

		return nil
	}, nil
}

func StopMinioServer(stopMinioServer func() error) error {
	if stopMinioServer == nil {
		return nil
	}

	return stopMinioServer()
}
