package components

import (
	"fmt"
	"os"

	"github.com/litebase/litebase/pkg/cli"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/charmbracelet/lipgloss/v2/table"
	"github.com/charmbracelet/x/term"
)

type CardRow struct {
	Key   string
	Value string
}

var cardStyle = func() lipgloss.Style {
	return lipgloss.NewStyle().
		BorderForeground(cli.LightDark(cli.Gray400, cli.Gray500)).
		BorderLeft(true).
		BorderStyle(lipgloss.InnerHalfBlockBorder()).
		PaddingLeft(1)
}

var CardTitleStyle = func() lipgloss.Style {
	return lipgloss.NewStyle().
		Padding(0, 1).
		Bold(true).
		Background(cli.LightDark(cli.Sky700, cli.Sky300)).
		Foreground(cli.LightDark(cli.White, cli.Black))
}

type Card struct {
	Description  string
	Content      string
	ContentTitle string
	Rows         []CardRow
	Title        string
	Width        int
}

type CardOption func(*Card)

func NewCard(options ...CardOption) *Card {
	c := &Card{}

	for _, opt := range options {
		opt(c)
	}

	width, _, err := term.GetSize(os.Stdout.Fd())

	if err != nil {
		width = 80
	}

	c.Width = width // Leave some margin

	return c
}

func WithCardDescription(description string) CardOption {
	return func(c *Card) {
		c.Description = description
	}
}

func WithCardContent(title, content string) CardOption {
	return func(c *Card) {
		c.ContentTitle = title
		c.Content = content
	}
}

func WithCardRows(rows []CardRow) CardOption {
	return func(c *Card) {
		c.Rows = rows
	}
}

func WithCardTitle(title string) CardOption {
	return func(c *Card) {
		c.Title = title
	}
}

func (c *Card) Render() string {
	content := ""

	if c.Title != "" {
		content += CardTitleStyle().Render(c.Title)
	}

	if c.Description != "" {
		content += lipgloss.NewStyle().
			MarginTop(2).
			Width(c.Width - 4).
			Render(c.Description)
	}

	rows := [][]string{}

	for _, row := range c.Rows {
		rows = append(rows, []string{fmt.Sprintf("%s: ", row.Key), row.Value})
	}

	t := table.New().
		Border(lipgloss.Border{}).
		StyleFunc(func(row, col int) lipgloss.Style {
			switch col {
			case 0:
				return lipgloss.NewStyle().Bold(true)
			default:
				return lipgloss.NewStyle()
			}
		}).
		Rows(rows...)

	tableContent := t.Render()

	// Join rows with newlines
	if len(tableContent) > 0 {
		content += lipgloss.NewStyle().
			MarginTop(1).
			Render(lipgloss.JoinVertical(
				lipgloss.Left,
				tableContent,
			))
	}

	if c.Content != "" {
		content += c.renderContent()
	}

	// Apply the card style with proper width to prevent border breaking
	return cardStyle().Render(content)
}

func (c *Card) renderContent() string {
	content := lipgloss.NewStyle().Bold(true).MarginTop(2).Render(c.ContentTitle)

	renderer, err := glamour.NewTermRenderer(
		glamour.WithAutoStyle(),
		glamour.WithWordWrap(c.Width-2),
	)

	if err != nil {
		return "Error initializing renderer: " + err.Error()
	}

	glamourContent, err := renderer.Render(c.Content)

	if err != nil {
		glamourContent = "Error rendering content: " + err.Error()
	}

	content += glamourContent

	return content
}
