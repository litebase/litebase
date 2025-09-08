package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewTokenDeleteCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Short: "Delete a token",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, _, err := api.Delete(config, fmt.Sprintf("/v1/tokens/%s", args[0]))

			if err != nil {
				return err
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

			return err
		},
	}
}
