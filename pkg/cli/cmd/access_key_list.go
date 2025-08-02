package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewAccessKeyListCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List access keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/access-keys")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No access keys found")),
				)

				return nil
			}

			rows := [][]string{}

			accessKeys, ok := data["data"].([]any)

			if !ok {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for access keys")),
				)

				return nil
			}

			for i, accessKey := range accessKeys {
				var accessKeyId string = "-"

				if a, ok := accessKey.(map[string]any)["access_key_id"].(string); ok {
					accessKeyId = a
				}

				// Ensure access_key_id is a string before appending
				rows = append(rows, []string{
					fmt.Sprintf("%d", i+1),
					accessKeyId,
				})
			}

			columns := []string{
				"#",
				"Access Key ID",
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							accessKeyShow(cmd, config, row[1])
						}).Render(config.GetInteractive()),
				),
			)

			return nil
		},
	}
}
