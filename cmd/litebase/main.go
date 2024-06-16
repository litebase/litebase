package main

import (
	"litebase/cli/cmd"
	"log"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cmd.NewRoot()
}
