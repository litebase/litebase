package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

func NewDatabaseListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List databases",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get("/resources/databases")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				return fmt.Errorf("no databases found")
			}

			rows := [][]string{}

			for _, database := range data["data"].([]any) {
				rows = append(rows, []string{
					database.(map[string]any)["id"].(string),
					database.(map[string]any)["name"].(string),
				})
			}

			components.NewTable([]string{"ID", "Name"}, rows).Render()

			return nil
		},
	}

}
