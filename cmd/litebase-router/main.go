package main

import (
	"litebase/router"
	"log"

	"github.com/joho/godotenv"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	r := router.NewRouter()
	r.Start()
}
