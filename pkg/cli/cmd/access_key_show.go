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

	dataMap, ok := res["data"].(map[string]any)

	if !ok {
		return fmt.Errorf("invalid response format")
	}

	var cardContent string

	rows := []components.CardRow{}

	if accessKeyIdStr, ok := dataMap["accessKeyId"].(string); ok {
		rows = append(rows, components.CardRow{
			Key:   "Access Key ID",
			Value: accessKeyIdStr,
		})
	}

	if dataMap["description"] != nil {
		if description, ok := dataMap["description"].(string); ok {
			rows = append(rows, components.CardRow{
				Key:   "Description",
				Value: description,
			})
		}
	}

	if dataMap["createdAt"] != nil {
		if createdAtStr, ok := dataMap["createdAt"].(string); ok {
			parsedDate, err := time.Parse(time.RFC3339, createdAtStr)

			if err != nil {
				return err
			}

			rows = append(rows, components.CardRow{
				Key:   "Created At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if dataMap["updatedAt"] != nil {
		if updatedAtStr, ok := dataMap["updatedAt"].(string); ok {
			parsedDate, err := time.Parse(time.RFC3339, updatedAtStr)

			if err != nil {
				return err
			}

			rows = append(rows, components.CardRow{
				Key:   "Updated At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if dataMap["statements"] != nil {
		if statements, ok := dataMap["statements"].([]any); ok {
			statementsJSON, err := json.MarshalIndent(statements, "", "  ")

			if err != nil {
				return err
			}

			cardContent = "```json\n" + string(statementsJSON) + "\n```"
		}
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
