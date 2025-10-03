package openapi

import (
	"sort"
	"strings"
)

// GetTags returns the tag definitions for organizing API endpoints
// This is now dynamically generated based on the actual tags used in the specification
func GetTags() []Tag {
	// This will be populated dynamically by the generator
	// based on actual controller analysis results
	return []Tag{}
}

// GenerateDynamicTags creates tag definitions based on actual controller analysis
// Both tag names and descriptions are generated dynamically from controller patterns
func GenerateDynamicTags(usedTags map[string]bool) []Tag {
	var tags []Tag

	// Generate tags only for the ones actually used
	for tagName := range usedTags {
		tags = append(tags, Tag{
			Name:        tagName,
			Description: generateTagDescription(tagName),
		})
	}

	// Sort tags lexicographically by name for consistent output
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Name < tags[j].Name
	})

	return tags
}

// generateTagDescription dynamically generates descriptions from tag names
func generateTagDescription(tagName string) string {
	// Convert CamelCase to words and generate meaningful descriptions
	words := splitCamelCase(tagName)

	// Special handling for compound words
	if len(words) >= 2 {
		// Handle patterns like "DatabaseBackup" -> "Database backup operations"
		if strings.ToLower(words[len(words)-1]) == "backup" {
			return strings.Join(words[:len(words)-1], " ") + " backup operations"
		}

		if strings.ToLower(words[len(words)-1]) == "restore" {
			return strings.Join(words[:len(words)-1], " ") + " restore operations"
		}

		if strings.ToLower(words[len(words)-1]) == "snapshot" {
			return strings.Join(words[:len(words)-1], " ") + " snapshot operations"
		}

		if strings.ToLower(words[len(words)-1]) == "branch" {
			return strings.Join(words[:len(words)-1], " ") + " branch management operations"
		}

		if strings.ToLower(words[len(words)-1]) == "log" {
			return strings.Join(words[:len(words)-1], " ") + " performance and usage metrics"
		}

		if strings.ToLower(words[len(words)-1]) == "stream" {
			return "Streaming " + strings.ToLower(strings.Join(words[:len(words)-1], " ")) + " operations"
		}

		if strings.ToLower(words[len(words)-1]) == "connection" {
			return strings.Join(words[:len(words)-1], " ") + " connection management operations"
		}

		if strings.ToLower(words[len(words)-1]) == "election" {
			return strings.Join(words[:len(words)-1], " ") + " leader election operations"
		}

		if strings.ToLower(words[len(words)-1]) == "status" {
			return strings.Join(words[:len(words)-1], " ") + " status and health operations"
		}

		if strings.ToLower(words[len(words)-1]) == "primary" {
			return "Primary " + strings.ToLower(strings.Join(words[:len(words)-1], " ")) + " node operations"
		}

		if strings.ToLower(words[len(words)-1]) == "activate" {
			return strings.Join(words[:len(words)-1], " ") + " activation operations"
		}
	}

	// Handle single words or general patterns
	switch strings.ToLower(tagName) {
	case "user":
		return "User management operations"
	case "token":
		return "Token management for authentication"
	case "accesskey":
		return "Access key management for authentication"
	case "database":
		return "Database management operations"
	case "query":
		return "SQL query execution operations"
	case "key":
		return "Encryption key management"
	case "healthcheck":
		return "Health check endpoints"
	}

	// Generic fallback: convert to readable format
	return strings.Join(words, " ") + " operations"
}
