package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var DatabaseBackupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Manage database backups",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := cmd.Help()

		if err != nil {
			return err
		}

		return nil
	},
}

var DatabaseBackupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new database backup",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

var DatabaseBackupDeleteCmd = &cobra.Command{
	Use:  "delete <id>",
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func NewDatabaseBackupCmd(config *config.Configuration) *cobra.Command {
	DatabaseBackupCmd.AddCommand(DatabaseBackupCreateCmd)
	DatabaseBackupCmd.AddCommand(DatabaseBackupDeleteCmd)

	return DatabaseBackupCmd
}
