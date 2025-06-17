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
		Run: func(cmd *cobra.Command, args []string) {
			data, err := api.Get("/resources/databases")

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			if data["data"] == nil {
				fmt.Print(components.Container(components.WarningAlert("No databases found")))
				return
			}

			rows := [][]string{}

			for _, database := range data["data"].([]interface{}) {
				rows = append(rows, []string{
					database.(map[string]interface{})["id"].(string),
					database.(map[string]interface{})["name"].(string),
				})
			}

			components.NewTable([]string{"ID", "Name"}, rows).Render()
		},
	}

}
