package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseListCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List databases",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/resources/databases")

			if err != nil {
				return err
			}

			if data["data"] == nil {
				return fmt.Errorf("no databases found")
			}

			rows := [][]string{}

			for _, database := range data["data"].([]any) {
				rows = append(rows, []string{
					database.(map[string]any)["database_id"].(string),
					database.(map[string]any)["name"].(string),
				})
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable([]string{"ID", "Name"}, rows).
						Render(config.GetInteractive()),
				),
			)

			return nil
		},
	}

}
