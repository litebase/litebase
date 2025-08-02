package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseCmd(config *config.Configuration) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "database",
		Short: "Manage databases",
		Args:  cobra.MinimumNArgs(1),
	}

	cmd.AddCommand(NewDatabaseCreateCmd(config))
	cmd.AddCommand(NewDatabaseDeleteCmd(config))
	cmd.AddCommand(NewDatabaseListCmd(config))
	cmd.AddCommand(NewDatabaseShowCmd(config))

	cmd.AddCommand(NewDatabaseBackupCmd(config))
	cmd.AddCommand(NewDatabaseRestoreCmd(config))
	cmd.AddCommand(NewDatabaseUpdateCmd(config))
	cmd.AddCommand(NewDatabaseQueryCmd(config))
	cmd.AddCommand(NewDatabaseQueryLogCmd(config))

	return cmd
}
