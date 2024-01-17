package file

import (
	"fmt"
	"litebasedb/internal/config"
	"path/filepath"
	"strings"
)

func DatabaseDirectory() string {
	return fmt.Sprintf("%s/.litebasedb/_databases", config.Get().DataPath)
}

func GetFileDir(databaseUuid string, branchUuid string) string {
	dir, err := GetFilePath(databaseUuid, branchUuid)

	if err != nil {
		return ""
	}

	return filepath.Dir(dir)
}

func GetFilePath(databaseUuid string, branchUuid string) (string, error) {
	path := fmt.Sprintf("%s/%s/%s", DatabaseDirectory(), databaseUuid, branchUuid)

	pathParts := strings.Split(path, "/")

	// Insert without replacing the branchuuid to the path before the last segement.
	pathParts = append(pathParts[:len(pathParts)-1], append([]string{branchUuid}, pathParts[len(pathParts)-1:]...)...)

	path = strings.Join(pathParts, "/")

	return fmt.Sprintf("%s/%s", strings.TrimRight(config.Get().DataPath, "/"), strings.TrimLeft(path, "/")), nil
}
