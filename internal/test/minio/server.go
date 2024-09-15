package minio

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"github.com/minio/madmin-go/v3"
	minio "github.com/minio/minio/cmd"
)

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
