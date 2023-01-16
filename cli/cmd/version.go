package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of LitebaseDB",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("LitebaseDB CLI v0.0.1")
	},
}
