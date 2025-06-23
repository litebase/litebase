package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

func NewAccessKeyListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List access keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get("/resources/access-keys")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				fmt.Print(components.Container(components.WarningAlert("No databases found")))
				return nil
			}

			rows := [][]string{}

			for _, accessKey := range data["data"].([]any) {
				rows = append(rows, []string{
					accessKey.(map[string]any)["access_key_id"].(string),
					"",
				})
			}

			components.NewTable([]string{"Access Key ID", "Name"}, rows).Render()

			return nil
		},
	}
}
