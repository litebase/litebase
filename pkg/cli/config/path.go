package config

import (
	"os"
	"path/filepath"
)

// ConfigPath returns the config path of the CLI based on the current working directory.
// It first checks if the current directory has a .litebase directory with a config file.
// If found, it returns the path to that local config file, otherwise it returns the default global configuration path.
func ConfigPath() (string, error) {
	localConfigPath := "./.litebase/config.yml"

	if _, err := os.Stat(localConfigPath); err == nil {
		return localConfigPath, nil
	}

	return DefaultConfigPath()
}

// DefaultConfigPath returns the default global configuration path.
// This is typically something like ~/.litebase/config.yml on Unix-like systems,
// but the exact path can vary based on the operating system.
func DefaultConfigPath() (string, error) {
	homeDir, err := os.UserHomeDir()

	if err != nil {
		return "", err
	}

	return filepath.Join(homeDir, ".litebase", "config.yml"), nil
}
