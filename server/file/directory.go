package file

import (
	"os"
	"path/filepath"
)

func EnsureDirectoryExists(path string) error {
	directory := filepath.Dir(path)

	_, err := os.Stat(directory)

	if os.IsNotExist(err) {
		if err := os.MkdirAll(directory, 0755); err != nil {
			return err
		}

	} else if err != nil {
		return err
	}

	return nil
}
