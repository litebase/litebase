package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseBranchListCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "list <database>",
		Args:  cobra.ExactArgs(1),
		Short: "List database branches",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName := args[0]

			data, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/branches", databaseName))

			if err != nil {
				return err
			}

			if data["data"] == nil {
				return fmt.Errorf("no branches found for database %s", databaseName)
			}

			rows := [][]string{}

			for _, branch := range data["data"].([]any) {
				branchData := branch.(map[string]any)
				name := branchData["name"].(string)
				branchID := branchData["databaseBranchId"].(string)

				parentName := ""

				if parentNameValue, exists := branchData["parentName"]; exists && parentNameValue != nil {
					parentName = parentNameValue.(string)
				}

				rows = append(rows, []string{
					name,
					branchID,
					parentName,
				})
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable([]string{"Name", "Branch ID", "Parent"}, rows).
						Render(config.GetInteractive()),
				),
			)

			return err
		},
	}
}
