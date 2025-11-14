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

func userShow(cmd *cobra.Command, config *config.CLIConfiguration, AccessKeyID string) error {
	res, err := api.Get(config, fmt.Sprintf("/v1/users/%s", AccessKeyID))

	if err != nil {
		return err
	}

	dataMap, ok := res["data"].(map[string]any)

	if !ok {
		return fmt.Errorf("invalid response format")
	}

	var cardContent string
	rows := []components.CardRow{}

	if username, ok := dataMap["username"].(string); ok {
		rows = append(rows, components.CardRow{
			Key:   "User Name",
			Value: username,
		})
	}

	if createdAt, ok := dataMap["createdAt"].(string); ok && createdAt != "" {
		parsedDate, err := time.Parse(time.RFC3339, createdAt)

		if err == nil {
			rows = append(rows, components.CardRow{
				Key:   "Created At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if updatedAt, ok := dataMap["updatedAt"].(string); ok && updatedAt != "" {
		parsedDate, err := time.Parse(time.RFC3339, updatedAt)

		if err == nil {
			rows = append(rows, components.CardRow{
				Key:   "Updated At",
				Value: parsedDate.Format(time.RFC3339),
			})
		}
	}

	if statements, ok := dataMap["statements"].([]any); ok && statements != nil {
		statementsJSON, err := json.MarshalIndent(statements, "", "  ")
		if err == nil {
			cardContent = "```json\n" + string(statementsJSON) + "\n```"
		}
	}

	_, err = lipgloss.Fprint(
		cmd.OutOrStdout(),
		components.Container(
			components.NewCard(
				components.WithCardTitle("User"),
				components.WithCardRows(rows),
				components.WithCardContent("Statements", cardContent),
			).Render(),
		),
	)

	return err
}

func NewUserShowCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <username>",
		Short: "Show user details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return userShow(cmd, config, args[0])
		},
	}
}
