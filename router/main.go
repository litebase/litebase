package main

import (
	"log"

	"github.com/joho/godotenv"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	err := godotenv.Load(".env.router")

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	NewApp().Serve()
}
