package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseQueryCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "query <database_id> <query>",
		Short: "Execute a query on a database",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseId := args[0]
			query := args[1]

			res, _, err := api.Post(config, fmt.Sprintf("/v1/databases/%s/query", databaseId), map[string]any{"query": query})
			if err != nil {
				return err
			}

			fmt.Println("Query executed successfully:", res)
			return nil
		},
	}
}
