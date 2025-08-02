package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseUpdateCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "update <id>",
		Short: "Update an existing database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// databaseId := args[0]
			return nil
		},
	}
}
