package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewUserUpdateCmd(config *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update <username>",
		Short: "Update a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var confirmed bool
			username := args[0]

			input := UserInput{Statements: []UserInputStatement{}}

			statements, err := cmd.Flags().GetString("statements")

			if err != nil {
				return err
			}

			// Check if we're in non-interactive mode (flags provided)
			nonInteractive := !config.GetInteractive() || statements != ""

			if nonInteractive {
				if err := json.Unmarshal([]byte(statements), &input.Statements); err != nil {
					return errors.New("invalid JSON format for statements")
				}

				confirmed = true
			} else {
				res, err := api.Get(config, fmt.Sprintf("/v1/users/%s", username))

				if err != nil {
					return err
				}

				if res["data"].(map[string]any)["statements"] != nil {
					statements := res["data"].(map[string]any)["statements"].([]any)
					statementsJSON, err := json.MarshalIndent(statements, "", "  ")

					if err != nil {
						return err
					}

					if err := json.Unmarshal(statementsJSON, &input.Statements); err != nil {
						return errors.New("invalid JSON format for statements")
					}
				}

				statementsJSON, err := json.MarshalIndent(input.Statements, "", "  ")

				if err != nil {
					log.Fatal("Error marshalling statements to JSON:", err)
				}

				statementsString := string(statementsJSON)

				form := components.NewForm(
					huh.NewGroup(
						huh.NewNote().
							Title("Update User").
							Description("Update the statements for the user"),
						huh.NewText().
							Title("Statements").
							Description("Define privileges for this user using JSON").
							Lines(10).
							Value(&statementsString).
							Validate(func(str string) error {
								// Ensure not empty
								if str == "" {
									return errors.New("statements cannot be empty")
								}

								// Ensure valid JSON
								var statements []UserInputStatement

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
							Description("Are you sure you want to update this user?").
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

			res, _, err := api.Put(config, fmt.Sprintf("/v1/users/%s", username), map[string]any{
				"statements": input.Statements,
			})

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
					components.SuccessAlert(res["message"].(string)),
					components.NewCard(
						components.WithCardTitle("User"),
						components.WithCardRows(rows),
						components.WithCardContent("Statements", cardContent),
					).Render(),
				),
			)

			return nil
		},
	}

	// Add flags
	cmd.Flags().String("statements", "", "JSON array of statements")

	return cmd
}
