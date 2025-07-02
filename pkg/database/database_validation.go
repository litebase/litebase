package database

import "fmt"

type DatabaseName string

func (r DatabaseName) Validate() error {
	if r == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	if len(r) < 3 || len(r) > 64 {
		return fmt.Errorf("database name must be between 3 and 64 characters")
	}

	// Database names can contain alphanumeric characters, underscores, and hyphens
	for _, char := range r {
		if !(('a' <= char && char <= 'z') || ('A' <= char && char <= 'Z') || ('0' <= char && char <= '9') || char == '_' || char == '-') {
			return fmt.Errorf("database name can only contain alphanumeric characters, underscores, and hyphens")
		}
	}

	return nil
}
