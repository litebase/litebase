//go:build !production
// +build !production

package storage

import (
	"context"
	"fmt"
	"litebase/internal/config"
	"log"
	"os"
	"os/exec"
	"syscall"
)

var minioCmd *exec.Cmd
var minioCmdCtx context.Context
var minioCmdCancel context.CancelFunc

func StartTestS3Server() {
	minioCmdCtx, minioCmdCancel = context.WithCancel(context.Background())
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
	minioCmd = exec.CommandContext(minioCmdCtx, "minio", "server", storageDirectory, "--address", ":9000") // "--quiet"

	// Redirect i/o
	minioCmd.Stdout = os.Stdout
	minioCmd.Stderr = os.Stderr

	// Ignore signals in the command process
	minioCmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
		Pgid:    0,
	}

	err = minioCmd.Start()
	if err != nil {
		log.Fatalf("failed to start minio server, %v", err)
	}

	// Ensure the bucket exists
	ObjectFS().Driver().(*ObjectFileSystemDriver).EnsureBucketExists()
}

func StopTestS3Server() {
	if minioCmd != nil {
		minioCmdCancel()

		// minioCmd.Process.Signal(syscall.SIGKILL)

		// log.Println("Stopping test s3 server")
		// err := minioCmd.Process.Kill()

		// if err != nil {
		// 	log.Fatalf("failed to stop minio server, %v", err)
		// }

		log.Println("Stopped test s3 server")
	}
}
