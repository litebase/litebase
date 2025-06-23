package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var DatabaseQueryLogCmd = &cobra.Command{
	Use:   "query-logs",
	Short: "View database query logs",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Database query logs:", args[0])

		return nil
	},
}

func NewDatabaseQueryLogListCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List database query logs",
		RunE: func(cmd *cobra.Command, args []string) error {
			// fmt.Println("Database query logs:", []string{"db1", "db2", "db3"})
			return nil
		},
	}
}

func NewDatabaseQueryLogCmd(config *config.Configuration) *cobra.Command {
	DatabaseQueryLogCmd.AddCommand(NewDatabaseQueryLogListCmd(config))

	return DatabaseQueryLogCmd
}
