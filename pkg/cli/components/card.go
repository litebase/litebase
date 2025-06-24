package components

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli"

	"github.com/charmbracelet/lipgloss/v2"
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
		PaddingBottom(1).
		PaddingLeft(1)
}

var CardTitleStyle = func() lipgloss.Style {
	return lipgloss.NewStyle().
		MarginBottom(1).
		Padding(0, 1).
		Bold(true).
		Background(cli.LightDark(cli.Sky700, cli.Sky300)).
		Foreground(cli.LightDark(cli.White, cli.Black))
}

var cardRowStyle = lipgloss.NewStyle().
	PaddingTop(1)

type Card struct {
	Rows  []CardRow
	Title string
}
type CardOption func(*Card)

func NewCard(options ...CardOption) *Card {
	c := &Card{}

	for _, opt := range options {
		opt(c)
	}

	return c
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

func (c *Card) View() string {
	content := ""
	maxKeyLength := 0

	if c.Title != "" {
		content += CardTitleStyle().Render(c.Title)
	}

	for _, row := range c.Rows {
		if len(row.Key) >= maxKeyLength {
			maxKeyLength = len(row.Key)
		}
	}

	for _, row := range c.Rows {
		key := row.Key

		if len(row.Key) < maxKeyLength {
			key += fmt.Sprintf("%*s", maxKeyLength-len(row.Key), "")
		}

		content += cardRowStyle.Render(
			lipgloss.NewStyle().Bold(true).Render(key),
			lipgloss.NewStyle().Render(row.Value),
		)
	}

	return cardStyle().Render(content)
}
