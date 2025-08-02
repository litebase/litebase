package cmd

import (
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewUserDeleteCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <username>",
		Short: "Delete a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, _, err := api.Delete(config, "/v1/users/"+args[0])

			if err != nil {
				return err
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert("User deleted successfully"),
				),
			)

			return nil
		},
	}
}
