package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseDeleteCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a database",
		RunE: func(cmd *cobra.Command, args []string) error {
			res, _, err := api.Delete(config, fmt.Sprintf("/v1/databases/%s", args[0]))

			if err != nil {
				return err
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

			return nil
		},
	}
}
