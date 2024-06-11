package main

import (
	"litebase/router"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	r := router.NewRouter()
	r.Start()
}
