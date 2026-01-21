package cmd

import (
	"errors"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseCreateCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <database>",
		Args:  cobra.ExactArgs(1),
		Short: "Create a new database",
		RunE: func(cmd *cobra.Command, args []string) error {
			data := map[string]any{"name": args[0]}

			if primaryBranch, _ := cmd.Flags().GetString("primary-branch"); primaryBranch != "" {
				data["primaryBranch"] = primaryBranch
			}

			if encrypted, _ := cmd.Flags().GetBool("encrypted"); encrypted {
				data["encrypted"] = true
			}

			res, apiErrors, err := api.Post(config, "/v1/databases", data)

			if err != nil {
				return err
			}

			if len(apiErrors) > 0 {
				return apiErrors.Error()
			}

			message := "Database created successfully"

			if msg, ok := res["message"].(string); ok {
				message = msg
			}

			// Safely extract data map
			dataMap, ok := res["data"].(map[string]any)
			if !ok {
				return errors.New("invalid response format")
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(message),
					components.DatabaseCard(dataMap),
				),
			)

			return err
		},
	}

	cmd.Flags().String("primary-branch", "", "The name of the primary branch for the database")
	cmd.Flags().Bool("encrypted", false, "Enable encryption for the database (requires LITEBASE_DATA_ENCRYPTION_KEY on server)")

	return cmd
}
