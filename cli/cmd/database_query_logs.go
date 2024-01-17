package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var DatabaseQueryLogCmd = &cobra.Command{
	Use:   "query-logs",
	Short: "View database query logs",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Database query logs:", args[0])
	},
}

var DatabaseQueryLogListCmd = &cobra.Command{
	Use:   "list",
	Short: "List database query logs",
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("Database query logs:", []string{"db1", "db2", "db3"})
	},
}

func NewDatabaseQueryLogCmd() *cobra.Command {
	DatabaseQueryLogCmd.AddCommand(DatabaseQueryLogListCmd)

	return DatabaseQueryLogCmd
}
