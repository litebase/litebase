package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var DatabaseRestoreCmd = &cobra.Command{
	Use:   "restore <id>",
	Args:  cobra.ExactArgs(1),
	Short: "Restore a database",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Database restored:", args[0])
	},
}

func NewDatabaseRestoreCmd() *cobra.Command {
	return DatabaseRestoreCmd
}
