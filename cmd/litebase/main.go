package main

import (
	"log"

	"github.com/litebase/litebase/pkg/cli/cmd"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := cmd.NewRoot()

	if err != nil {
		log.Fatal(err)
	}
}
