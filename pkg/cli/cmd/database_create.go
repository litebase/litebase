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

func NewDatabaseCreateCmd(config *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <name> --primary-branch <branch_name>",
		Args:  cobra.ExactArgs(1),
		Short: "Create a new database",
		RunE: func(cmd *cobra.Command, args []string) error {
			data := map[string]any{"name": args[0]}

			if primaryBranch, _ := cmd.Flags().GetString("primary-branch"); primaryBranch != "" {
				data["primary_branch"] = primaryBranch
			}

			res, _, err := api.Post(config, "/v1/databases", data)

			if err != nil {
				_, fprintErr := fmt.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert(err.Error())),
				)

				if fprintErr != nil {
					slog.Error("Error printing error message", "error", fprintErr)
				}

				return err
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseCard(res["data"].(map[string]any)),
				),
			)

			return err
		},
	}

	cmd.Flags().String("primary-branch", "", "The name of the primary branch for the database")

	return cmd
}
