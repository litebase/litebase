package file

import (
	"log"
	"os"
	"path/filepath"
)

func EnsureDirectoryExists(path string) error {
	directory := filepath.Dir(path)

	_, err := os.Stat(directory)

	if os.IsNotExist(err) {
		if err := os.MkdirAll(directory, 0750); err != nil {
			log.Println("Failed to create directory:", directory, "Error:", err)
			return err
		}
	} else if err != nil {
		return err
	}

	return nil
}
