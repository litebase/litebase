package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseRestoreCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "restore <id>",
		Args:  cobra.ExactArgs(1),
		Short: "Restore a database",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("Database restored:", args[0])

			return nil
		},
	}
}
