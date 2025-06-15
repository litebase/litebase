package cmd

import "github.com/spf13/cobra"

var DatabaseBackupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Manage database backups",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var DatabaseBackupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new database backup",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
	},
}

var DatabaseBackupDeleteCmd = &cobra.Command{
	Use:  "delete <id>",
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func NewDatabaseBackupCmd() *cobra.Command {
	DatabaseBackupCmd.AddCommand(DatabaseBackupCreateCmd)
	DatabaseBackupCmd.AddCommand(DatabaseBackupDeleteCmd)

	return DatabaseBackupCmd
}
