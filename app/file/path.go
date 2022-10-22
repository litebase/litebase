package file

import (
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/config"
	"log"
	"path/filepath"
	"strings"
)

func GetFileDir(databaseUuid string, branchUuid string) string {
	dir, err := GetFilePath(databaseUuid, branchUuid)
	log.Println("GetFilePath: ", dir, err)

	if err != nil {
		return ""
	}

	return filepath.Dir(dir)
}

func GetFilePath(databaseUuid string, branchUuid string) (string, error) {
	path, err := auth.SecretsManager().GetPath(databaseUuid, branchUuid)

	if err != nil {
		return "", err
	}

	pathParts := strings.Split(path, "/")

	// Insert without replacing the branchuuid to the path before the last segement.
	pathParts = append(pathParts[:len(pathParts)-1], append([]string{branchUuid}, pathParts[len(pathParts)-1:]...)...)

	path = strings.Join(pathParts, "/")

	return strings.TrimRight(config.Get("data_path"), "/") + "/" + strings.TrimLeft(path, "/"), nil
}
