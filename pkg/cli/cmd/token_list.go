package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewTokenListCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List tokens",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/tokens")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No tokens found")),
				)

				return nil
			}

			rows := [][]string{}

			tokens, ok := data["data"].([]any)

			if !ok {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for tokens")),
				)

				return nil
			}

			for i, token := range tokens {
				var tokenId string = "-"

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

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							tokenShow(cmd, config, row[1])
						}).Render(config.GetInteractive()),
				),
			)

			return nil
		},
	}
}
