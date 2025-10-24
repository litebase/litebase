package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBranchCreateCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <database/branch> <new-branch>",
		Args:  cobra.ExactArgs(2),
		Short: "Create a new database branch",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, parentBranchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			newBranchName := args[1]

			data := map[string]any{
				"name": newBranchName,
			}

			if parentBranchName != "" {
				data["parentName"] = parentBranchName
			}

			res, _, err := api.Post(config, fmt.Sprintf("/v1/databases/%s/branches", databaseName), data)

			if err != nil {
				return err
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseBranchCard(res["data"].(map[string]any)),
				),
			)

			return err
		},
	}

	return cmd
}
