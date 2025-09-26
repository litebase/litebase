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

func NewTokenListCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List tokens",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/tokens")

			if err != nil {
				return err
			}

			if data["data"] == nil || len(data["data"].([]any)) == 0 {
				_, err := lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No tokens found")),
				)

				return err
			}

			rows := [][]string{}

			tokens, ok := data["data"].([]any)

			if !ok {
				_, err := lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for tokens")),
				)

				return err
			}

			for i, token := range tokens {
				var tokenId = "-"

				if t, ok := token.(map[string]any)["token_id"].(string); ok {
					tokenId = t
				}

				// Ensure token_id is a string before appending
				rows = append(rows, []string{
					fmt.Sprintf("%d", i+1),
					tokenId,
				})
			}

			columns := []string{
				"#",
				"Token ID",
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							err := tokenShow(cmd, config, row[1])

							if err != nil {
								slog.Error("Error showing token", "error", err, "token_id", row[1])
							}
						}).Render(config.GetInteractive()),
				),
			)

			return err
		},
	}
}
