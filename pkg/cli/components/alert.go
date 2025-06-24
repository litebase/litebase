package components

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/styles"
)

func ErrorAlert(message string) string {
	return fmt.Sprintf("%s → %s", styles.AlertDangerStyle.Render("Error"), message)
}

func SuccessAlert(message string) string {
	return fmt.Sprintf("%s → %s", styles.AlertSuccessStyle.Render("Success"), message)
}

func WarningAlert(message string) string {
	return fmt.Sprintf("%s → %s", styles.AlertWarningStyle.Render("Warning"), message)
}
