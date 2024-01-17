package cmd

import (
	"fmt"
	"litebasedb/cli/api"
	"litebasedb/cli/components"
	"litebasedb/server/auth"
	"strings"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

type UserListResponse struct {
	Data []auth.User `json:"data"`
}

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

type model struct {
	table table.Model
}

func (m model) Init() tea.Cmd { return nil }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.table.SetWidth(msg.Width)

		return m, nil
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focus()
			}
		case "q", "ctrl+c":
			return m, tea.Quit
		case "enter":
			return m, tea.Batch(
				tea.Printf("Let's go to %s!", m.table.SelectedRow()[0]),
			)
		}
	}

	m.table, cmd = m.table.Update(msg)

	return m, cmd
}

func (m model) View() string {
	return baseStyle.Render(m.table.View()) + "\n"
}

func NewUserListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List users",
		Run: func(cmd *cobra.Command, args []string) {
			res, err := api.Get("/users")

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			columns := []string{
				"Username",
				"Privileges",
				"Created At",
				"Updated At",
			}

			var users []auth.User

			for _, user := range res["data"].([]interface{}) {
				privileges := []string{}

				for _, priv := range user.(map[string]interface{})["privileges"].([]interface{}) {
					privileges = append(privileges, priv.(string))
				}

				users = append(users, auth.User{
					Username:   user.(map[string]interface{})["username"].(string),
					Privileges: privileges,
					CreatedAt:  user.(map[string]interface{})["created_at"].(string),
					UpdatedAt:  user.(map[string]interface{})["updated_at"].(string),
				})
			}

			rows := [][]string{}

			for _, user := range users {
				priv := user.Privileges

				rows = append(rows, []string{
					user.Username,
					strings.Join(priv, ", "),
					user.CreatedAt,
					user.UpdatedAt,
				})
			}

			components.NewTable(columns, rows).Render()
		},
	}
}
