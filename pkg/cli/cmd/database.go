package cmd

import (
	"github.com/spf13/cobra"
)

var DatabaseCmd = &cobra.Command{
	Use:   "database",
	Short: "Manage databases",
	Args:  cobra.MinimumNArgs(1),
}

func NewDatabaseCmd() *cobra.Command {
	DatabaseCmd.AddCommand(NewDatabaseCreateCmd())
	DatabaseCmd.AddCommand(NewDatabaseDeleteCmd())
	DatabaseCmd.AddCommand(NewDatabaseListCmd())
	DatabaseCmd.AddCommand(NewDatabaseShowCmd())

	DatabaseCmd.AddCommand(NewDatabaseBackupCmd())
	DatabaseCmd.AddCommand(NewDatabaseRestoreCmd())
	DatabaseCmd.AddCommand(NewDatabaseSettingsCmd())
	DatabaseCmd.AddCommand(NewDatabaseQueryLogCmd())

	return DatabaseCmd
}
