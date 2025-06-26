package components

import (
	"os"

	"github.com/litebase/litebase/pkg/cli"

	"github.com/charmbracelet/glamour"
	"github.com/charmbracelet/lipgloss/v2"
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
	maxKeyLength := 0

	// Calculate available width for content
	availableWidth := c.Width

	if availableWidth <= 0 {
		availableWidth = 80 // Default width if not specified
	}

	// Account for border and padding (border=1, paddingLeft=1)
	contentWidth := availableWidth - 2

	if c.Title != "" {
		title := c.Title

		// Truncate title if it's too long
		if len(title) > contentWidth {
			title = truncateString(title, contentWidth)
		}

		content += CardTitleStyle().Render(title)
	}

	if c.Description != "" {
		description := c.Description

		// Wrap description with proper word wrapping
		wrappedDescription := lipgloss.NewStyle().
			MarginTop(2).
			Width(contentWidth - 2).
			Render(description)

		content += wrappedDescription
	}

	// Find the optimal key length
	for _, row := range c.Rows {
		if len(row.Key) > maxKeyLength {
			maxKeyLength = len(row.Key)
		}
	}

	// Adjust maxKeyLength if it would take up too much space
	// Reserve at least 20 characters for the value, or half the width, whichever is smaller
	maxValueWidth := contentWidth - maxKeyLength - 1 // -1 for space between key and value

	if maxValueWidth < 20 && contentWidth > 30 {
		maxKeyLength = contentWidth - 21 // Reserve 20 chars for value + 1 for space
	} else if maxKeyLength > contentWidth/2 {
		maxKeyLength = contentWidth / 2
	}

	var rowStrings []string

	for _, row := range c.Rows {
		key := row.Key
		value := row.Value

		// Truncate key if too long
		if len(key) > maxKeyLength {
			key = truncateString(key, maxKeyLength)
		}

		// Calculate remaining space for value
		remainingWidth := contentWidth - len(key) - 1 // -1 for space between key and value

		// Truncate value if too long
		if len(value) > remainingWidth && remainingWidth > 0 {
			value = truncateString(value, remainingWidth)
		}

		// Build the row string with exact width control
		rowContent := lipgloss.NewStyle().Bold(true).Render(key) + " " + value

		// Ensure the row doesn't exceed content width
		if lipgloss.Width(rowContent) > contentWidth {
			// If rendered width is still too long, truncate more aggressively
			totalLength := len(key) + 1 + len(value)

			if totalLength > contentWidth {
				newValueLength := contentWidth - len(key) - 1

				if newValueLength > 0 {
					value = truncateString(value, newValueLength)
				}

				rowContent = lipgloss.NewStyle().Bold(true).Render(key) + " " + value
			}
		}

		rowStrings = append(rowStrings, rowContent)
	}

	// Join rows with newlines
	if len(rowStrings) > 0 {
		content += lipgloss.NewStyle().
			MarginTop(2).
			Render(lipgloss.JoinVertical(
				lipgloss.Left,
				rowStrings...,
			))
	}

	if c.Content != "" {
		content += c.renderContent()
	}

	// Apply the card style with proper width to prevent border breaking
	return cardStyle().
		Width(contentWidth).
		Render(content)
}

func (c *Card) renderContent() string {
	// Calculate available width for content (same as in Render method)
	availableWidth := c.Width
	if availableWidth <= 0 {
		availableWidth = 80
	}
	contentWidth := availableWidth - 2

	content := lipgloss.NewStyle().Bold(true).MarginTop(2).Render(c.ContentTitle)

	renderer, err := glamour.NewTermRenderer(
		glamour.WithAutoStyle(),
		glamour.WithWordWrap(contentWidth),
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
