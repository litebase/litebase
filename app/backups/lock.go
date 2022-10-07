package backups

import (
	"os"
	"path/filepath"
)

type Lock struct {
	path string
}

func NewLock(path string) *Lock {
	lock := &Lock{
		path: path,
	}

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path), 0755)
	}

	os.WriteFile(path, []byte{}, 0644)

	return lock
}

func (l *Lock) Release() {
	os.Remove(l.path)
}
