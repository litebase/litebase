//go:build darwin

package storage

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func openFileDirect(name string, flag int, perm os.FileMode) (*os.File, error) {
	// #nosec G304
	file, err := os.OpenFile(filepath.Clean(name), flag, perm)

	if err != nil {
		return nil, err
	}

	fd := file.Fd()

	_, err = unix.FcntlInt(uintptr(fd), unix.F_NOCACHE, 1)

	if err != nil {
		return nil, err
	}

	return file, nil
}
