package main

import (
	"log"

	"github.com/joho/godotenv"
	"github.com/litebase/litebase/pkg/router"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	r := router.NewRouter()
	r.Start()
}
