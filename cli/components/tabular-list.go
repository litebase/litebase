package components

import (
	"fmt"
	"litebase/cli/styles"

	"github.com/charmbracelet/lipgloss"
)

var keyStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#9ecbff"})

var valueStyle = lipgloss.NewStyle().
	Underline(true)

func TabularList(listSlice []map[string]string) string {
	var minSpace = 16
	var maxLength = 0

	for _, item := range listSlice {
		if len(item["value"]) > maxLength {
			maxLength = len(item["value"])
		}
	}

	var listItems string

	var last = len(listSlice)

	for _, item := range listSlice {
		var lineBreak = "\n\n"
		var spacerLength = maxLength + minSpace - (len(item["key"]) + len(item["value"]))
		var spacer string = ""
		last -= 1

		for i := 0; i < spacerLength; i++ {
			spacer += "･"
		}

		if last == 0 {
			lineBreak = ""
		}

		listItems += fmt.Sprintf(
			"%s %s %s%s",
			keyStyle.Render(item["key"]),
			styles.LineSpacerStyle.Render(spacer),
			valueStyle.Render(item["value"]),
			lineBreak,
		)
	}

	return listItems
}
