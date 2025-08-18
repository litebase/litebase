package auth

import (
	"strings"
)

type Resource string

// Check if the access key resource has a prefix.
func (r Resource) HasPrefix(prefix string) bool {
	return strings.HasPrefix(string(r), prefix)
}

// Validate if the access key resource is valid.
func (r Resource) IsValid() bool {
	if r == "*" {
		return true
	}

	// Check if it's a pattern with colon (e.g., "resource:*" or "resource:value")
	parts := strings.Split(string(r), ":")

	if len(parts) >= 1 {
		// Check if the base resource (part before first colon) exists
		baseResource := parts[0]

		_, exists := Resources[baseResource]

		return exists
	}

	return false
}
