package storage

import (
	"log"
	"os"
)

type TempDatabaseFileSystem struct {
	path string
}

func NewTempDatabaseFileSystem(path, databaseUuid, branchUuid string) *TempDatabaseFileSystem {
	fs := TmpFS()

	// Check if the the directory exists
	if _, err := fs.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := fs.MkdirAll(path, 0755); err != nil {
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
