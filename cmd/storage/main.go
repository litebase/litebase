package main

import (
	"litebasedb/storage"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	s := storage.Storage{}
	s.Start()
}
