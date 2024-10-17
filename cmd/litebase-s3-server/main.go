package main

import (
	"litebase/internal/config"
	"litebase/server/storage"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	godotenv.Load(".env")
	os.Setenv("LITEBASE_STORAGE_OBJECT_MODE", "object")
	config := config.NewConfig()

	objectFS := storage.NewFileSystem(
		storage.NewObjectFileSystemDriver(
			config,
		),
	)

	url, err := storage.StartTestS3Server(config, objectFS)

	if err != nil {
		log.Fatalf("Failed to start test s3 server: %v", err)
	}

	log.Printf("Test S3 server started at %s", url)
	log.Println("Server started")
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	<-signals

	storage.StopTestS3Server()
}
