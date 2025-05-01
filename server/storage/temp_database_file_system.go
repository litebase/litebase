package storage

import (
	"log"
	"os"
)

type TempDatabaseFileSystem struct {
	path string
}

func NewTempDatabaseFileSystem(tmpFS *FileSystem, path, databaseId, branchId string) *TempDatabaseFileSystem {
	// Check if the the directory exists
	if _, err := tmpFS.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := tmpFS.MkdirAll(path, 0755); err != nil {
				log.Fatalln("Error creating temp file system directory", err)
			}
		} else {
			log.Fatalln("Error checking temp file system directory", err)
		}
	}

	return &TempDatabaseFileSystem{
		path: path,
	}
}

func (tfs *TempDatabaseFileSystem) Path() string {
	return tfs.path
}
