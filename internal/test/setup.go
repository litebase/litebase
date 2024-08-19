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

	err := godotenv.Load("./../../.env.test")
	os.Setenv("LITEBASE_SIGNATURE", CreateHash(64))

	// config.Get().SignatureNext = CreateHash(32)
	server.NewApp(server.NewServer())

	if err != nil {
		t.Fail()
	}
}

func Teardown() {
	os.Setenv("LITEBASE_SIGNATURE", "")
	database.ConnectionManager().Shutdown()
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
