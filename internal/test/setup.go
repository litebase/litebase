package test

import (
	"litebasedb/internal/config"
	"litebasedb/server"
	"litebasedb/server/database"
	"litebasedb/server/storage"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func Setup(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Setenv("LITEBASEDB_DATA_PATH", "../../data/_test")
	err := godotenv.Load("../../.env")

	config.Get().Signature = CreateHash(32)
	config.Get().SignatureNext = CreateHash(32)
	server.NewApp(server.NewServer())

	config.Get().DataPath = "../../data/_test"
	config.Get().TmpPath = "../../data/_test/tmp"

	if err != nil {
		t.Fail()
	}

}

func Teardown() {
	database.ConnectionManager().Shutdown()
	err := storage.FS().RemoveAll("./../../data/_test")

	if err != nil {
		log.Fatal(err)
	}
}

func Run(t *testing.T, callback func()) {
	Setup(t)
	callback()
	Teardown()
}
