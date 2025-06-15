package components

import "github.com/litebase/litebase/pkg/cli/styles"

func ErrorAlert(message string) string {
	return styles.AlertDangerStyle.Render(message)
}

func SuccessAlert(message string) string {
	return styles.AlertSuccessStyle.Render(message)
}

func WarningAlert(message string) string {
	return styles.AlertWarningStyle.Render(message)
}
