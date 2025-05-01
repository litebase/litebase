package components

import (
	"fmt"

	"github.com/litebase/litebase/cli/styles"

	"github.com/charmbracelet/lipgloss"
)

type CardRow struct {
	Key   string
	Value string
}

var cardStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240")).
	Width(64).
	Padding(1)

var cardTitleStyle = lipgloss.NewStyle().
	MarginBottom(1).
	Padding(0, 1).
	Bold(true).
	Background(styles.PrimaryBackgroundColor).
	Foreground(styles.PrimaryForegroundColor)

var cardRowStyle = lipgloss.NewStyle().
	Padding(1)

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
		content += cardTitleStyle.Render(c.Title)
	}

	for _, row := range c.Rows {
		if len(row.Key) >= maxKeyLength {
			maxKeyLength = len(row.Key)
		}
	}

	for i, row := range c.Rows {
		key := row.Key

		if len(row.Key) < maxKeyLength {
			key += fmt.Sprintf("%*s", maxKeyLength-len(row.Key), "")
		}

		style := cardRowStyle.Copy()

		if i == len(c.Rows)-1 {
			style = style.PaddingBottom(0)
		}

		content += style.Render(
			lipgloss.NewStyle().Bold(true).Render(key),
			lipgloss.NewStyle().Render(row.Value),
		)
	}

	return cardStyle.Render(content)
}
