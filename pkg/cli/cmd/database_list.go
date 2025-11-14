package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseListCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List databases",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "/v1/databases")

			if err != nil {
				return err
			}

			databases, ok := data["data"].([]any)

			if !ok || len(databases) == 0 {
				return fmt.Errorf("no databases found")
			}

			rows := [][]string{}

			for _, database := range databases {
				dbMap, ok := database.(map[string]any)

				if !ok {
					continue
				}

				name, _ := dbMap["databaseName"].(string)
				id, _ := dbMap["databaseId"].(string)

				rows = append(rows, []string{name, id})
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable([]string{"Name", "ID"}, rows).
						Render(config.GetInteractive()),
				),
			)

			return err
		},
	}

}
