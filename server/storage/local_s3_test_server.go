//go:build !production
// +build !production

package storage

import (
	"fmt"
	"log"
	"net"
	"net/http/httptest"
	"os"

	"github.com/litebase/litebase/common/config"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3afero"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/spf13/afero"
)

var s3Server *httptest.Server

func s3Faker(c *config.Config) *gofakes3.GoFakeS3 {
	var backend gofakes3.Backend
	var err error

	if c.Env == config.EnvDevelopment {
		backend, err = s3afero.MultiBucket(
			afero.NewBasePathFs(
				afero.NewOsFs(),
				fmt.Sprintf("%s/_object_storage", os.Getenv("LITEBASE_LOCAL_DATA_PATH")),
			),
		)

		if err != nil {
			log.Fatalf("failed to create test s3 server, %v", err)
		}
	} else {
		backend = s3mem.New()
	}

	faker := gofakes3.New(backend)

	return faker
}

func StartTestS3Server(c *config.Config, objectFS *FileSystem) (string, error) {
	faker := s3Faker(c)

	s3Server = httptest.NewUnstartedServer(faker.Server())

	if c.Env == config.EnvDevelopment {
		// Create a new listener on your desired port
		listener, err := net.Listen("tcp", "127.0.0.1:4444")

		if err != nil {
			log.Fatalf("failed to create test s3 server, %v", err)
		}

		s3Server.Listener = listener
	}

	s3Server.Start()
	c.StorageEndpoint = s3Server.URL
	objectFS.Driver().(*ObjectFileSystemDriver).S3Client.Endpoint = s3Server.URL

	// Ensure the bucket exists
	objectFS.Driver().(*ObjectFileSystemDriver).EnsureBucketExists()

	return s3Server.URL, nil
}

func StopTestS3Server() {
	s3Server.Close()
}
