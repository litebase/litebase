package cmd

import (
	"fmt"
	"log/slog"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewAccessKeyListCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List access keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/access-keys")

			if err != nil {
				return err
			}

			if data["data"] == nil || len(data["data"].([]any)) == 0 {
				_, err := lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No access keys found")),
				)

				return err
			}

			rows := [][]string{}

			accessKeys, ok := data["data"].([]any)

			if !ok {
				_, outputErr := lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for access keys")),
				)

				if outputErr != nil {
					slog.Error("Error printing access keys", "error", outputErr)
				}

				return nil
			}

			for i, accessKey := range accessKeys {
				var accessKeyId = "-"

				if a, ok := accessKey.(map[string]any)["accessKeyId"].(string); ok {
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

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							err := accessKeyShow(cmd, config, row[1])

							if err != nil {
								slog.Error("Error showing access key", "error", err)
							}
						}).Render(config.GetInteractive()),
				),
			)

			return err
		},
	}
}
