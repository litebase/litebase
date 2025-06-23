package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var DatabaseCmd = &cobra.Command{
	Use:   "database",
	Short: "Manage databases",
	Args:  cobra.MinimumNArgs(1),
}

func NewDatabaseCmd(config *config.Configuration) *cobra.Command {
	DatabaseCmd.AddCommand(NewDatabaseCreateCmd(config))
	DatabaseCmd.AddCommand(NewDatabaseDeleteCmd(config))
	DatabaseCmd.AddCommand(NewDatabaseListCmd(config))
	DatabaseCmd.AddCommand(NewDatabaseShowCmd(config))

	DatabaseCmd.AddCommand(NewDatabaseBackupCmd(config))
	DatabaseCmd.AddCommand(NewDatabaseRestoreCmd(config))
	DatabaseCmd.AddCommand(NewDatabaseSettingsCmd(config))
	DatabaseCmd.AddCommand(NewDatabaseQueryLogCmd(config))

	return DatabaseCmd
}
