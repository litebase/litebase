package test

import (
	"fmt"
	"litebase/internal/config"
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

func Setup(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := godotenv.Load("./../../.env.test")

	if err != nil {
		log.Fatal(err)
	}

	if envDataPath == "" {
		envDataPath = os.Getenv("LITEBASE_DATA_PATH")
	}

	dataPath := fmt.Sprintf("%s/%s", envDataPath, CreateHash(64))
	tmpPath := fmt.Sprintf("%s/_tmp", dataPath)

	os.MkdirAll(dataPath, 0755)
	os.MkdirAll(tmpPath, 0755)

	t.Setenv("LITEBASE_DATA_PATH", dataPath)
	t.Setenv("LITEBASE_TMP_PATH", tmpPath)
	t.Setenv("LITEBASE_SIGNATURE", CreateHash(64))

	// config.Get().SignatureNext = CreateHash(32)
	server.NewApp(server.NewServer())

	if err != nil {
		t.Fail()
	}

}

func Teardown() {
	database.ConnectionManager().Shutdown()
	node.Node().Shutdown()

	err := storage.FS().RemoveAll(config.Get().DataPath)

	if err != nil {
		log.Fatal(err)
	}
}

func Run(t *testing.T, callback func()) {
	Setup(t)
	callback()
	Teardown()
}
