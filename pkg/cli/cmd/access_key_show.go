package cmd

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func accessKeyShow(cmd *cobra.Command, config *config.CLIConfiguration, accessKeyId string) error {
	res, err := api.Get(config, fmt.Sprintf("/v1/access-keys/%s", accessKeyId))

	if err != nil {
		return err
	}

	var cardContent string

	rows := []components.CardRow{
		{
			Key:   "Access Key ID",
			Value: res["data"].(map[string]any)["accessKeyId"].(string),
		},
	}

	if res["data"].(map[string]any)["description"] != nil {
		rows = append(rows, components.CardRow{
			Key:   "Description",
			Value: res["data"].(map[string]any)["description"].(string),
		})
	}

	if res["data"].(map[string]any)["createdAt"] != nil {
		parsedDate, err := time.Parse(time.RFC3339, res["data"].(map[string]any)["createdAt"].(string))

		if err != nil {
			return err
		}

		rows = append(rows, components.CardRow{
			Key:   "Created At",
			Value: parsedDate.Format(time.RFC3339),
		})
	}

	if res["data"].(map[string]any)["updatedAt"] != nil {
		parsedDate, err := time.Parse(time.RFC3339, res["data"].(map[string]any)["updatedAt"].(string))

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
		statementsJSON, err := json.MarshalIndent(statements, "", "  ")

		if err != nil {
			return err
		}

		cardContent = "```json\n" + string(statementsJSON) + "\n```"
	}

	_, err = lipgloss.Fprint(
		cmd.OutOrStdout(),
		components.Container(
			components.NewCard(
				components.WithCardTitle("Access Key"),
				components.WithCardRows(rows),
				components.WithCardContent("Statements", cardContent),
			).Render(),
		),
	)

	return err
}

func NewAccessKeyShowCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Show access key details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return accessKeyShow(cmd, config, args[0])
		},
	}
}
