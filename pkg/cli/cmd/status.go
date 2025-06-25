package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewStatusCmd(c *config.Configuration) *cobra.Command {
	return NewCommand("status", "Show the status of the cluster").
		WithRunE(func(cmd *cobra.Command, args []string) error {
			res, err := api.Get(c, "/status")

			if err != nil {
				return err
			}

			var alert string

			switch res["status"].(string) {
			case "ok":
				alert = components.SuccessAlert(res["message"].(string))
			case "warning":
				alert = components.WarningAlert(res["message"].(string))
			case "error":
				alert = components.ErrorAlert(res["message"].(string))
			default:
				alert = components.InfoAlert(res["status"].(string))
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					alert,
					components.NewCard(
						components.WithCardTitle("Cluster Status"),
						components.WithCardRows([]components.CardRow{
							{
								Key:   "Node Count",
								Value: fmt.Sprintf("%d", int(res["data"].(map[string]any)["node_count"].(float64))),
							},
						}),
					).Render(),
				),
			)

			return nil
		}).Build()
}
