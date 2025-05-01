package main

import (
	"log"

	"github.com/litebase/litebase/cli/cmd"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cmd.NewRoot()
}
