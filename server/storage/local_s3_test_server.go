//go:build !production
// +build !production

package storage

import (
	"fmt"
	"log"
	"net"
	"net/http/httptest"
	"os"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/spf13/afero"
)

var s3Server *httptest.Server

func StartTestS3Server() {
	backend, err := s3afero.MultiBucket(
		afero.NewBasePathFs(
			afero.NewOsFs(),
			fmt.Sprintf("%s/_object_storage", os.Getenv("LITEBASE_LOCAL_DATA_PATH")),
		),
	)

	if err != nil {
		log.Fatalf("failed to create test s3 server, %v", err)
	}

	faker := gofakes3.New(backend)
	s3Server = httptest.NewUnstartedServer(faker.Server())

	listener, err := net.Listen("tcp", ":9000")

	if err != nil {
		log.Printf("failed to create test s3 server, %v", err)

		return
	}

	s3Server.Listener = listener

	s3Server.Start()

	// Ensure the bucket exists
	ObjectFS().Driver().(*ObjectFileSystemDriver).EnsureBucketExists()

	log.Println("Started test s3 server")
}

func StopTestS3Server() {
	s3Server.Close()
	log.Println("Stopped test s3 server")
}
