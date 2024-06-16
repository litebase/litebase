package test

import (
	"litebase/internal/config"
	"litebase/server"
	"litebase/server/database"
	"litebase/server/storage"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
)

func Setup(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	os.Setenv("LITEBASE_DATA_PATH", "../../data/_test")
	os.Setenv("LITEBASE_SIGNATURE", CreateHash(32))
	err := godotenv.Load("../../.env")

	// config.Get().SignatureNext = CreateHash(32)
	server.NewApp(server.NewServer())

	config.Get().DataPath = "../../data/_test"
	config.Get().TmpPath = "../../data/_test/tmp"

	if err != nil {
		t.Fail()
	}
}

func Teardown() {
	os.Setenv("LITEBASE_SIGNATURE", "")
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
