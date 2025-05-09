//go:build !linux && !darwin

package storage

import (
	"os"
)

func openFileDirect(name string, flag int, perm os.FileMode) (*os.File, error) {
	// No-op for non-Linux and non-Darwin systems, use standard open file

	file, err := os.OpenFile(name, flag, perm)

	if err != nil {
		return nil, err
	}

	return file, nil
}
