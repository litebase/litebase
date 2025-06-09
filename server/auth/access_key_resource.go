package auth

import (
	"strings"
)

type AccessKeyResource string

// Check if the access key resource has a prefix.
func (r AccessKeyResource) HasPrefix(prefix string) bool {
	return strings.HasPrefix(string(r), prefix)
}

// Validate if the access key resource is valid.
func (r AccessKeyResource) IsValid() bool {
	if r == "*" {
		return true
	}

	// Check if it's a pattern with colon (e.g., "resource:*" or "resource:value")
	parts := strings.Split(string(r), ":")

	if len(parts) >= 1 {
		// Check if the base resource (part before first colon) exists
		baseResource := parts[0]

		_, exists := AccessKeyResources[baseResource]

		return exists
	}

	return false
}
