package auth

import (
	"slices"
	"strings"
)

// resourceMatches checks if a resource pattern matches a given resource
func resourceMatches(pattern, resource string) bool {
	// Universal wildcard
	if pattern == "*" {
		return true
	}

	// Exact match
	if pattern == resource {
		return true
	}

	// Pattern with trailing wildcard (e.g., "resource:*")
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(resource, prefix)
	}

	return false
}

// Determine if an Access Key is authorized to perform an action on a resource.
func Authorized(statements []AccessKeyStatement, resource string, permission Privilege) bool {
	// Order the statements in descending order based on the number of
	// segmentations in the resource (most specific first)
	slices.SortFunc(statements, func(a, b AccessKeyStatement) int {
		return strings.Count(string(b.Resource), ":") - strings.Count(string(a.Resource), ":")
	})

	var allowFound bool

	for _, statement := range statements {
		// Check if the statement resource matches the requested resource, or is a wildcard
		if !resourceMatches(string(statement.Resource), resource) {
			continue
		}

		// Check if the statement allows all actions
		if len(statement.Actions) == 1 && statement.Actions[0] == "*" {
			if strings.ToLower(string(statement.Effect)) == "deny" {
				return false // Deny always takes precedence
			}

			if strings.ToLower(string(statement.Effect)) == "allow" {
				allowFound = true
			}

			continue
		}

		// Check if the statement allows the specific permission
		if slices.Contains(statement.Actions, permission) {
			if strings.ToLower(string(statement.Effect)) == "deny" {
				return false // Deny always takes precedence
			}

			if strings.ToLower(string(statement.Effect)) == "allow" {
				allowFound = true
			}
		}
	}

	if allowFound {
		return true
	}

	// Deny by default if no statement matches
	return false
}
