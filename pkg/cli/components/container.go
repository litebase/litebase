package components

import "github.com/charmbracelet/lipgloss/v2"

func Container(content ...string) string {
	text := ""

	for i, t := range content {
		var marginTop int = 1
		var marginBottom int = 0

		if i == 0 {
			marginTop = 2
		}

		if i == len(content)-1 {
			marginBottom = 1
		}

		text += lipgloss.NewStyle().
			MarginTop(marginTop).
			MarginBottom(marginBottom).
			Render(t)
	}

	return lipgloss.NewStyle().
		Render(text)
}
