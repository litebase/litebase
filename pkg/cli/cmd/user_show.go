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

func userShow(cmd *cobra.Command, config *config.Configuration, AccessKeyId string) error {
	res, err := api.Get(config, fmt.Sprintf("/resources/users/%s", AccessKeyId))

	if err != nil {
		return err
	}

	var cardContent string

	rows := []components.CardRow{
		{
			Key:   "User Name",
			Value: res["data"].(map[string]any)["username"].(string),
		},
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
		statementsJSON, err := json.MarshalIndent(statements, "", "  ")

		if err != nil {
			return err
		}

		cardContent = "```json\n" + string(statementsJSON) + "\n```"
	}

	lipgloss.Fprint(
		cmd.OutOrStdout(),
		components.Container(
			components.NewCard(
				components.WithCardTitle("User"),
				components.WithCardRows(rows),
				components.WithCardContent("Statements", cardContent),
			).Render(),
		),
	)

	return nil
}

func NewUserShowCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <username>",
		Short: "Show user details",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return userShow(cmd, config, args[0])
		},
	}
}
