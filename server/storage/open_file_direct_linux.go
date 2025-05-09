//go:build linux

package storage

import (
	"os"
	"syscall"
)

func openFileDirect(name string, flag int, perm os.FileMode) (*os.File, error) {
	// Add the O_DIRECT flag to the file open flags
	// Ensure O_DIRECT is only used with compatible flags
	if flag&syscall.O_DIRECT == 0 {
		flag |= syscall.O_DIRECT
	}

	file, err := os.OpenFile(name, flag, perm)

	if err != nil {
		return nil, err
	}

	return file, nil
}
