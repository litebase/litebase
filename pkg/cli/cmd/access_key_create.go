package cmd

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

type AccessKeyInput struct {
	Description string                    `json:"description"`
	Statements  []AccessKeyInputStatement `json:"statements"`
}

type AccessKeyInputStatement struct {
	Effect   auth.AccessKeyEffect `json:"effect"`
	Resource string               `json:"resource"`
	Actions  []string             `json:"actions"`
}

func NewAccessKeyCreateCmd(config *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a new access key",
		RunE: func(cmd *cobra.Command, args []string) error {
			var confirmed bool

			input := AccessKeyInput{
				Description: "",
				Statements: []AccessKeyInputStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []string{"*"},
					},
				},
			}

			description, err := cmd.Flags().GetString("description")

			if err != nil {
				return err
			}

			statements, err := cmd.Flags().GetString("statements")

			if err != nil {
				return err
			}

			// Check if we're in non-interactive mode (flags provided)
			nonInteractive := !config.GetInteractive() || (description != "" && statements != "")

			if nonInteractive {
				// Non-interactive mode: use provided flags
				input.Description = description

				if err := json.Unmarshal([]byte(statements), &input.Statements); err != nil {
					return errors.New("invalid JSON format for statements")
				}

				confirmed = true
			} else {
				// Interactive mode: show form
				if description != "" {
					input.Description = description
				}

				statementsJSON, err := json.MarshalIndent(input.Statements, "", "  ")
				if err != nil {
					log.Fatal("Error marshalling statements to JSON:", err)
				}

				statementsString := string(statementsJSON)

				form := components.NewForm(
					huh.NewGroup(
						huh.NewNote().
							Title("Create Access Key").
							Description("Add a description and define the statements for the access key."),
						huh.NewInput().
							Title("Description").
							Placeholder("What will this access key be used for?").
							Value(&input.Description).
							CharLimit(255),
					),
					huh.NewGroup(
						huh.NewText().
							Title("Statements").
							Description("Define privileges for this access key using JSON").
							Lines(10).
							Value(&statementsString).
							Validate(func(str string) error {
								// Ensure not empty
								if str == "" {
									return errors.New("statements cannot be empty")
								}

								// Ensure valid JSON
								var statements []AccessKeyInputStatement

								if err := json.Unmarshal([]byte(str), &statements); err != nil {
									return errors.New("invalid JSON format")
								}

								if len(statements) == 0 {
									return errors.New("statements cannot be empty")
								}

								return nil
							}),
					),
					huh.NewGroup(
						huh.NewConfirm().
							Title("Confirm").
							Description("Are you sure you want to create this access key?").
							Value(&confirmed),
					),
				)

				err = form.Run()

				if err != nil {
					log.Fatal(err)
				}

				// Parse the statements from the form input
				if err := json.Unmarshal([]byte(statementsString), &input.Statements); err != nil {
					return errors.New("invalid JSON format for statements")
				}
			}

			if !confirmed {
				return nil
			}

			res, _, err := api.Post(config, "/v1/access-keys", map[string]any{
				"description": input.Description,
				"statements":  input.Statements,
			})

			if err != nil {
				return err
			}

			var cardContent string

			rows := []components.CardRow{
				{
					Key:   "Access Key ID",
					Value: res["data"].(map[string]any)["access_key_id"].(string),
				},
				{
					Key:   "Access Key Secret",
					Value: res["data"].(map[string]any)["access_key_secret"].(string),
				},
				{
					Key:   "Description",
					Value: res["data"].(map[string]any)["description"].(string),
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
					components.SuccessAlert(res["message"].(string)),
					components.NewCard(
						components.WithCardTitle("Access Key"),
						components.WithCardDescription("Copy and securely store the Access Key ID and Secret now. You won't be able to retrieve the secret later."),
						components.WithCardRows(rows),
						components.WithCardContent("Statements", cardContent),
					).Render(),
				),
			)

			return nil
		},
	}

	// Add flags
	cmd.Flags().String("description", "", "Description for the access key")
	cmd.Flags().String("statements", "", "JSON array of statements")

	return cmd
}
