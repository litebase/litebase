package cmd

import (
	"fmt"
	"litebasedb/cli/api"
	"litebasedb/cli/components"

	"github.com/spf13/cobra"
)

func NewAccessKeyListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List access keys",
		Run: func(cmd *cobra.Command, args []string) {
			data, err := api.Get("/access-keys")

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			if data["data"] == nil {
				fmt.Print(components.Container(components.WarningAlert("No databases found")))
				return
			}

			rows := [][]string{}

			for _, accessKey := range data["data"].([]interface{}) {
				rows = append(rows, []string{
					accessKey.(map[string]interface{})["access_key_id"].(string),
					"",
				})
			}

			components.NewTable([]string{"Access Key ID", "Name"}, rows).Render()
		},
	}
}
