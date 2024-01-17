package cmd

import (
	"litebasedb/cli/components"
	"litebasedb/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all profiles",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			profiles := config.GetProfiles()

			columns := []string{"Name", "Cluster"}

			rows := [][]string{}

			for _, profile := range profiles {
				rows = append(rows, []string{profile.Name, profile.Cluster})
			}

			components.NewTable(columns, rows).Render()
		},
	}
}
