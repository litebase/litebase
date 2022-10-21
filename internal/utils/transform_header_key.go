package utils

import "strings"

func TransformHeaderKey(key string) string {
	return strings.ReplaceAll(strings.ToLower(key), "_", "-")
}
