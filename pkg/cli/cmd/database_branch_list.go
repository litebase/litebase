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

			branches, ok := data["data"].([]any)

			if !ok {
				return fmt.Errorf("invalid response format: expected array of branches")
			}

			for _, branch := range branches {
				branchData, ok := branch.(map[string]any)

				if !ok {
					continue
				}

				name := "-"

				if n, ok := branchData["name"].(string); ok {
					name = n
				}

				branchID := "-"

				if id, ok := branchData["databaseBranchId"].(string); ok {
					branchID = id
				}

				parentName := ""
				if parentNameValue, exists := branchData["parentName"]; exists && parentNameValue != nil {
					if pn, ok := parentNameValue.(string); ok {
						parentName = pn
					}
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
