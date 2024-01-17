package components

import (
	"fmt"
	"litebasedb/cli/styles"
	"strings"
)

var InputErrorsStyle = styles.AlertDangerStyle.
	Copy().
	MarginTop(2).
	Padding(1, 1)

func InputErrors(errors []string) string {
	if len(errors) == 0 {
		return ""
	}

	writer := strings.Builder{}

	for _, err := range errors {
		writer.WriteString(fmt.Sprintf("‣ %s", err))
	}

	return InputErrorsStyle.Render(writer.String())
}
