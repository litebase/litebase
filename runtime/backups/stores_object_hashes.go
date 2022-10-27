package backups

import (
	"fmt"
	"litebasedb/runtime/file"
	"os"
	"path/filepath"
	"strings"
)

type StoresObjectHashes struct{}

func (s *StoresObjectHashes) GetPath(databaseUuid string, branchUuid string, timestamp int, hash string) string {
	return strings.Join([]string{
		file.GetFileDir(databaseUuid, branchUuid),
		BACKUP_DIR,
		fmt.Sprintf("%d", timestamp),
		BACKUP_OBJECT_DIR,
		hash[0:2],
		hash[2:],
	}, "/")
}

func (s *StoresObjectHashes) storeObjectHash(path string, data []byte) error {
	write := func() error {
		return os.WriteFile(path, data, 0644)
	}

	err := write()

	if err != nil && os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path), 0755)
		err = write()
	}

	return err
}
