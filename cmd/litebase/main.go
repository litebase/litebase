package main

import (
	"log"

	"github.com/litebase/litebase/pkg/cli/cmd"
)

var Version = "dev"

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	err := cmd.NewRoot(Version)

	if err != nil {
		// log.Fatal(err)
	}
}
