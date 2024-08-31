//go:build !production
// +build !production

package storage

import (
	"fmt"
	"litebase/internal/config"
	"log"
	"os"
	"os/exec"
	"time"
)

func StartTestS3Server() {
	clusterId := config.Get().ClusterId

	if clusterId == "" {
		panic("cluster id is required to create a test s3 server")
	}

	storageDirectory := fmt.Sprintf("%s/_object_storage", config.Get().DataPath)

	err := os.MkdirAll(storageDirectory, 0755)
	if err != nil {
		log.Fatalf("failed to create bucket directory, %v", err)
	}

	// Start the MinIO server as a separate process
	cmd := exec.Command("minio", "server", storageDirectory, "--address", ":9000") // "--quiet"

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Start()
	if err != nil {
		log.Fatalf("failed to start minio server, %v", err)
	}

	// Wait for a few seconds to ensure the server is up and running
	time.Sleep(5 * time.Second)

	config.Get().StorageEndpoint = "http://localhost:9000"

	// Ensure the bucket exists
	ObjectFS().Driver().(*ObjectFileSystemDriver).EnsureBucketExists()
}
