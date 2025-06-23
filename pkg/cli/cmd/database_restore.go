package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var DatabaseRestoreCmd = &cobra.Command{
	Use:   "restore <id>",
	Args:  cobra.ExactArgs(1),
	Short: "Restore a database",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Database restored:", args[0])

		return nil
	},
}

func NewDatabaseRestoreCmd(config *config.Configuration) *cobra.Command {
	return DatabaseRestoreCmd
}
