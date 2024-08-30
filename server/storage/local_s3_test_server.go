//go:build !production
// +build !production

package storage

import (
	"fmt"
	"litebase/internal/config"
	"log"
	"os"

	minioCmd "github.com/minio/minio/cmd"
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

	// Start the MinIO server
	go func() {
		minioCmd.Main([]string{
			"minio",
			"server",
			storageDirectory,
			"--address", ":9000",
			"--quiet",
		})
	}()

	config.Get().StorageEndpoint = "http://localhost:9000"

	// Ensure the bucket exists
	ObjectFS().Driver().(*ObjectFileSystemDriver).EnsureBucketExists()
}
