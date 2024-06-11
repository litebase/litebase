package backups

import (
	"fmt"
	"litebase/server/file"
	"litebase/server/storage"
	"os"
	"path/filepath"
	"strings"
)

type StoresObjectHashes struct{}

func (s *StoresObjectHashes) GetPath(databaseUuid string, branchUuid string, timestamp int64, hash string) string {
	return strings.Join([]string{
		file.GetDatabaseFileDir(databaseUuid, branchUuid),
		BACKUP_DIR,
		fmt.Sprintf("%d", timestamp),
		BACKUP_OBJECT_DIR,
		hash[0:2],
		hash[2:],
	}, "/")
}

func (s *StoresObjectHashes) storeObjectHash(path string, data []byte) error {
	write := func() error {
		return storage.FS().WriteFile(path, data, 0666)
	}

	err := write()

	if err != nil && os.IsNotExist(err) {
		storage.FS().MkdirAll(filepath.Dir(path), 0755)
		err = write()
	}

	return err
}
