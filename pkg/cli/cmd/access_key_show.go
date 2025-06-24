package cmd

import (
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewAccessKeyShowCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Show access key details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := api.Get(config, fmt.Sprintf("/resources/access-keys/%s", args[0]))

			if err != nil {
				return err
			}

			rows := []components.CardRow{
				{
					Key:   "Access Key ID",
					Value: res["data"].(map[string]any)["access_key_id"].(string),
				},
			}

			if res["data"].(map[string]any)["description"] != nil {
				rows = append(rows, components.CardRow{
					Key:   "Description",
					Value: res["data"].(map[string]any)["description"].(string),
				})
			}

			if res["data"].(map[string]any)["created_at"] != nil {
				parsedDate, err := time.Parse(time.RFC3339, res["data"].(map[string]any)["created_at"].(string))

				if err != nil {
					return err
				}

				rows = append(rows, components.CardRow{
					Key:   "Created At",
					Value: parsedDate.Format(time.RFC3339),
				})
			}

			if res["data"].(map[string]any)["updated_at"] != nil {
				parsedDate, err := time.Parse(time.RFC3339, res["data"].(map[string]any)["updated_at"].(string))

				if err != nil {
					return err
				}

				rows = append(rows, components.CardRow{
					Key:   "Updated At",
					Value: parsedDate.Format(time.RFC3339),
				})
			}

			if res["data"].(map[string]any)["statements"] != nil {
				statements := res["data"].(map[string]any)["statements"].([]any)

				for i, statement := range statements {
					statementMap := statement.(map[string]any)

					rows = append(rows, components.CardRow{
						Key:   fmt.Sprintf("Statement %d", i+1),
						Value: fmt.Sprintf("%s %s %s", statementMap["effect"].(string), statementMap["resource"].(string), statementMap["actions"].([]any)[0].(string)),
					})
				}
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewCard(
						components.WithCardTitle("Access Key"),
						components.WithCardRows(rows),
					).View(),
				),
			)

			return nil
		},
	}
}
