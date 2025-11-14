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

func tokenShow(cmd *cobra.Command, config *config.CLIConfiguration, tokenId string) error {
	res, err := api.Get(config, fmt.Sprintf("/v1/tokens/%s", tokenId))

	if err != nil {
		return err
	}

	dataMap, ok := res["data"].(map[string]any)

	if !ok {
		return fmt.Errorf("invalid response format")
	}

	var cardContent string

	rows := []components.CardRow{}

	if tokenIdStr, ok := dataMap["tokenId"].(string); ok {
		rows = append(rows, components.CardRow{
			Key:   "Token ID",
			Value: tokenIdStr,
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
				components.WithCardTitle("Token"),
				components.WithCardRows(rows),
				components.WithCardContent("Statements", cardContent),
			).Render(),
		),
	)

	return err
}

func NewTokenShowCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Show token details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return tokenShow(cmd, config, args[0])
		},
	}
}
