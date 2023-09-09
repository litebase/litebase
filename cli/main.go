package main

import (
	"fmt"
	"litebasedb/cli/cmd"
	"os"
)

func main() {
	if err := cmd.NewRoot(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
