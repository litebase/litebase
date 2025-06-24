package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
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

type UserInput struct {
	Username   string               `json:"username"`
	Password   string               `json:"password"`
	Statements []UserInputStatement `json:"statements"`
}

type UserInputStatement struct {
	Effect   auth.AccessKeyEffect `json:"effect"`
	Resource string               `json:"resource"`
	Actions  []string             `json:"actions"`
}

func NewClusterUserCreateCmd(config *config.Configuration) *cobra.Command {
	return NewCommand("create", "Create a new user").
		WithFlags(func(cmd *cobra.Command) {
			cmd.Flags().StringP("new-username", "n", "", "Username for the new user")
			cmd.Flags().StringP("new-password", "w", "", "Password for the new user")
			cmd.Flags().StringP("statements", "s", "", "JSON array of statements")
		}).
		WithRunE(func(cmd *cobra.Command, args []string) error {
			var confirmed bool

			input := UserInput{
				Username: "",
				Password: "",
				Statements: []UserInputStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []string{string(auth.ClusterPrivilegeManage)},
					},
				},
			}

			username, err := cmd.Flags().GetString("new-username")

			if err != nil {
				return err
			}

			password, err := cmd.Flags().GetString("new-password")
			if err != nil {
				return err
			}

			statements, err := cmd.Flags().GetString("statements")
			if err != nil {
				return err
			}

			// Check if we're in non-interactive mode (flags provided)
			nonInteractive := username != "" && password != "" && statements != ""

			if nonInteractive {
				// Non-interactive mode: use provided flags
				input.Username = username
				input.Password = password

				if err := json.Unmarshal([]byte(statements), &input.Statements); err != nil {
					return errors.New("invalid JSON format for statements")
				}

				confirmed = true
			} else {
				// Interactive mode: show form
				if username != "" {
					input.Username = username
				}

				if password != "" {
					input.Password = password
				}

				statementsJSON, err := json.MarshalIndent(input.Statements, "", "  ")
				if err != nil {
					log.Fatal("Error marshalling statements to JSON:", err)
				}

				statementsString := string(statementsJSON)

				form := components.NewForm(
					huh.NewGroup(
						huh.NewNote().
							Title("Create User").
							Description("Add a username, password and define the statements for the user."),
						huh.NewInput().
							Title("New Username").
							Placeholder("Enter a unique username for the new user").
							Value(&input.Username).
							CharLimit(255).
							Validate(func(str string) error {
								if str == "" {
									return errors.New("username cannot be empty")
								}
								return nil
							}),
						huh.NewInput().
							Title("New Password").
							Placeholder("Enter a strong password for the new user").
							Value(&input.Password).
							EchoMode(huh.EchoModePassword).
							Validate(func(str string) error {
								if str == "" {
									return errors.New("password cannot be empty")
								}
								if len(str) < 8 {
									return errors.New("password must be at least 8 characters")
								}
								return nil
							}),
					),
					huh.NewGroup(
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
							Description("Are you sure you want to create this user?").
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

			res, _, err := api.Post(config, "/resources/users", map[string]any{
				"username":   input.Username,
				"password":   input.Password,
				"statements": input.Statements,
			})

			if err != nil {
				return err
			}

			rows := []components.CardRow{
				{
					Key:   "Username",
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

			for i, statement := range res["data"].(map[string]any)["statements"].([]any) {
				statementMap := statement.(map[string]any)

				rows = append(rows, components.CardRow{
					Key:   fmt.Sprintf("Statement %d", i+1),
					Value: fmt.Sprintf("%s %s %s", statementMap["effect"].(string), statementMap["resource"].(string), statementMap["actions"].([]any)[0].(string)),
				})
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.NewCard(
						components.WithCardTitle("User"),
						components.WithCardRows(rows),
					).View(),
				),
			)

			return nil
		}).
		Build()
}
