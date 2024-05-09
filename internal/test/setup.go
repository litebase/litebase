package test

import (
	"litebasedb/internal/config"
	"litebasedb/server/database"
	"litebasedb/server/storage"
	"log"
	"testing"

	"github.com/joho/godotenv"
)

func Setup(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := godotenv.Load("../../.env")
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
