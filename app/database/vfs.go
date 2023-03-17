package database

import (
	"fmt"
	"litebasedb/app/sqlite3_vfs"
	"os"
	"strings"
	"time"
)

type VFS struct {
	connection *Connection
}

var VFSFiles = map[string]*File{}

func NewVFS(connection *Connection) sqlite3_vfs.VFS {
	return &VFS{
		connection: connection,
	}
}

func (v *VFS) Open(name string, flags sqlite3_vfs.OpenFlag) (sqlite3_vfs.File, sqlite3_vfs.OpenFlag, error) {
	if VFSFiles[name] != nil {
		return VFSFiles[name], flags, nil
	}

	var fileFlags int

	if flags&sqlite3_vfs.OpenExclusive != 0 {
		fileFlags |= os.O_EXCL
	}

	if flags&sqlite3_vfs.OpenCreate != 0 {
		fileFlags |= os.O_CREATE
	}

	if flags&sqlite3_vfs.OpenReadOnly != 0 {
		fileFlags |= os.O_RDONLY
	}

	if flags&sqlite3_vfs.OpenReadWrite != 0 {
		fileFlags |= os.O_RDWR
	}

	f, err := os.OpenFile(v.connection.Path, fileFlags, 0666)

	if err != nil && !os.IsExist(err) {
		return nil, 0, sqlite3_vfs.CantOpenError
	}

	VFSFiles[name] = &File{
		connection: v.connection,
		name:       name,
		path:       v.connection.Path,
		pointer:    f,
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

func (v *VFS) Delete(name string, dirSync bool) error {
	fileName := name
	err := os.Remove(fileName)

	return err
}

func (v *VFS) Access(name string, flags sqlite3_vfs.AccessFlag) (bool, error) {
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

	if flags == sqlite3_vfs.AccessExists {
		return exists, nil
	}

	return true, nil
}

func (v *VFS) FullPathname(name string) string {
	return name
}
