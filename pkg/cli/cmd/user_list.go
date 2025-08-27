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

func NewUserListCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List users",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/users")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				_, err = lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No users found")),
				)

				return err
			}

			rows := [][]string{}

			users, ok := data["data"].([]any)

			if !ok {
				_, err = lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for users")),
				)

				return err
			}

			for i, user := range users {
				var userName = "-"

				if a, ok := user.(map[string]any)["username"].(string); ok {
					userName = a
				}

				// Ensure username is a string before appending
				rows = append(rows, []string{
					fmt.Sprintf("%d", i+1),
					userName,
				})
			}

			columns := []string{
				"#",
				"Username",
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							err := userShow(cmd, config, row[1])

							if err != nil {
								slog.Error("Error showing user:", "error", err)
							}
						}).Render(config.GetInteractive()),
				),
			)

			return err
		},
	}
}
