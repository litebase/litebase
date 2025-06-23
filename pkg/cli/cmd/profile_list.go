package cmd

import (
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all profiles",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			profiles := config.GetProfiles()

			columns := []string{"Name", "Cluster"}

			rows := [][]string{}

			for _, profile := range profiles {
				rows = append(rows, []string{profile.Name, profile.Cluster})
			}

			components.NewTable(columns, rows).Render()

			return nil
		},
	}
}
