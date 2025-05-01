package main

import (
	"log"

	"github.com/joho/godotenv"
	"github.com/litebase/litebase/router"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	godotenv.Load(".env")

	r := router.NewRouter()
	r.Start()
}
