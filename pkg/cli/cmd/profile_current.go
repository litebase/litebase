package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Return the current profile",
		Args:  cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			profiles, err := config.GetCurrentProfile()

			if err != nil {
				return err
			}

			fmt.Print(
				components.Container(
					components.NewCard(
						components.WithCardTitle("Current Profile"),
						components.WithCardRows([]components.CardRow{
							{
								Key:   "Name",
								Value: profiles.Name,
							},
							{
								Key:   "Cluster",
								Value: profiles.Cluster,
							},
						}),
					).View(),
				),
			)

			return nil
		},
	}
}
