package test

import (
	"litebasedb/runtime/config"
	"litebasedb/runtime/database"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func Setup() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := godotenv.Load("../../.env")
	config.Set("data_path", "../../data/_test")
	config.Set("tmp_path", "../../data/_test/tmp")

	if err != nil {
		log.Fatal(err)
	}
}

func Teardown() {
	database.ClearDatabases()
	err := os.RemoveAll("./../../data/_test")

	if err != nil {
		log.Fatal(err)
	}
}

func Run(callback func()) {
	Setup()
	callback()
	Teardown()
}
