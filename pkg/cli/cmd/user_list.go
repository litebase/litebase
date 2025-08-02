package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewUserListCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List users",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/users")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No users found")),
				)

				return nil
			}

			rows := [][]string{}

			users, ok := data["data"].([]any)

			if !ok {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for users")),
				)

				return nil
			}

			for i, user := range users {
				var userName string = "-"

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

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							userShow(cmd, config, row[1])
						}).Render(config.GetInteractive()),
				),
			)

			return nil
		},
	}
}
