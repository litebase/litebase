package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var DatabaseCmd = &cobra.Command{
	Use:   "database",
	Short: "Manage databases",
	Args:  cobra.MinimumNArgs(1),
}

var DatabaseCreateCmd = &cobra.Command{
	Use:   "create [DATABASE_NAME]",
	Args:  cobra.ExactArgs(1),
	Short: "Create a new database",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Database created:", args[0])
	},
}

var DatabaseDeleteCmd = &cobra.Command{
	Use:   "delete [DATABASE_NAME]",
	Args:  cobra.ExactArgs(1),
	Short: "Delete a database",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Database deleted:", args[0])
	},
}

var DatabaseListCmd = &cobra.Command{
	Use:   "list",
	Short: "List databases",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Databases:", []string{"db1", "db2", "db3"})
	},
}

func NewDatabaseCmd() *cobra.Command {
	DatabaseCmd.AddCommand(DatabaseCreateCmd)
	DatabaseCmd.AddCommand(DatabaseDeleteCmd)
	DatabaseCmd.AddCommand(DatabaseListCmd)

	return DatabaseCmd
}
