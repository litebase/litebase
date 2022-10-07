package database

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/psanford/sqlite3vfs"
)

type VFS struct {
}

var VFSFiles = map[string]*File{}

func New() sqlite3vfs.VFS {
	return VFS{}
}

func (vfs VFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	if strings.HasSuffix(name, "journal") {
		name = os.Getenv("TMP_DIRECTORY") + "/" + name
	} else {
		name = os.Getenv("DATABASE_DIRECTORY") + "/" + name
	}

	if VFSFiles[name] != nil {
		return VFSFiles[name], flags, nil
	}

	// fmt.Println("Opening: ", name, flags)
	var fileFlags int

	if flags&sqlite3vfs.OpenExclusive != 0 {
		fileFlags |= os.O_EXCL
	}

	if flags&sqlite3vfs.OpenCreate != 0 {
		fileFlags |= os.O_CREATE
	}

	if flags&sqlite3vfs.OpenReadOnly != 0 {
		fileFlags |= os.O_RDONLY
	}

	if flags&sqlite3vfs.OpenReadWrite != 0 {
		fileFlags |= os.O_RDWR
	}

	f, err := os.OpenFile(name, fileFlags, 0644)

	if err != nil && !os.IsExist(err) {
		return nil, 0, sqlite3vfs.CantOpenError
	}

	VFSFiles[name] = &File{
		name:    name,
		pointer: f,
	}

	return VFSFiles[name], flags, nil
}

func CloseVFSFiles() error {
	for _, file := range VFSFiles {
		if file != nil {
			file.Close()

			delete(VFSFiles, file.name)
		}
	}

	return nil
}

func (vfs VFS) Delete(name string, dirSync bool) error {
	// start := time.Now()
	fileName := name //strings.ReplaceAll(name, ".db", "")
	err := os.Remove(fileName)
	// fmt.Println("Deleting: ", fileName, time.Since(start))
	return err
}

func (vfs VFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	if strings.HasSuffix(name, "-journal") || strings.HasSuffix(name, "-wal") {
		return false, nil
	}

	start := time.Now()

	exists := true

	_, err := os.Stat(name)

	if err != nil && os.IsNotExist(err) {
		exists = false
	} else if err != nil {
		return false, err
	}

	fmt.Println("Accessed: ", name, time.Since(start))

	if flags == sqlite3vfs.AccessExists {
		return exists, nil
	}

	return true, nil
}

func (vfs VFS) FullPathname(name string) string {
	return name
}
