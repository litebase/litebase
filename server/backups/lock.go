package backups

import (
	"litebase/server/storage"
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

	if _, err := storage.ObjectFS().Stat(filepath.Dir(path)); os.IsNotExist(err) {
		storage.ObjectFS().MkdirAll(filepath.Dir(path), 0755)
	}

	storage.ObjectFS().WriteFile(path, []byte{}, 0666)

	return lock
}

func (l *Lock) Release() {
	storage.ObjectFS().Remove(l.path)
}
