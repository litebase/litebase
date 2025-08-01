package components

import (
	"github.com/charmbracelet/lipgloss/v2"
)

func Container(content ...string) string {
	if len(content) == 0 {
		return ""
	}

	var parts []string

	// Add top spacing
	parts = append(parts, "")

	for i, t := range content {
		// Add the content
		parts = append(parts, t)

		// Add spacing between elements (except after the last one)
		if i < len(content)-1 {
			parts = append(parts, "")
		}
	}

	// Add bottom spacing
	parts = append(parts, "")

	// Join with newlines to create consistent spacing
	return lipgloss.NewStyle().MarginBottom(1).Render(
		lipgloss.JoinVertical(
			lipgloss.Left,
			parts...,
		),
	)
}
