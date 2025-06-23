package cmd

import (
	"fmt"
	"os"

	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileSwitchCmd(c *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "switch",
		Short: "Switch to a profile",
		RunE: func(cmd *cobra.Command, args []string) error {
			profiles := c.GetProfiles()

			columns := []string{"Name", "Cluster"}

			rows := [][]string{}

			for _, profile := range profiles {
				rows = append(rows, []string{profile.Name, profile.Cluster})
			}

			components.NewTable(columns, rows).
				SetHandler(func(row []string) {
					err := c.SwitchProfile(row[0])

					if err != nil {
						panic(err)
					}

					fmt.Print(
						components.Container(
							components.SuccessAlert(fmt.Sprintf("Profile switched to %s", row[0])),
						),
					)

					os.Exit(0)
				}).Render()

			return nil
		},
	}

}
