package backups

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/storage"
	"sort"
	"strconv"
	"strings"

	"log"
	"os"
	"time"
)

/*
A Backup is a complete logical snapshot of a database at a given point in time.
This data is derived from a Snapshot and can be used to restore a database.
*/
type Backup struct {
	dfs               *storage.DurableDatabaseFileSystem
	BranchUuid        string
	DatabaseUuid      string
	maxPartSize       int64
	SnapshotTimestamp int64
}

type BackupConfigCallback func(backup *Backup)

/*
Returns a Backup object for the given database and branch at a timestamp.
*/
func GetBackup(
	dfs *storage.DurableDatabaseFileSystem,
	databaseUuid string,
	branchUuid string,
	snapshotTimestamp int64,
) *Backup {
	backup := &Backup{
		BranchUuid:        branchUuid,
		DatabaseUuid:      databaseUuid,
		dfs:               dfs,
		SnapshotTimestamp: snapshotTimestamp,
	}

	return backup
}

/*
Returns next backup for the given database and branch relative to the given
timestamp provided.
*/
func GetNextBackup(
	dfs *storage.DurableDatabaseFileSystem,
	databaseUuid string,
	branchUuid string,
	snapshotTimestamp int64,
) *Backup {
	backups := make([]int64, 0)
	backupsDirectory := fmt.Sprintf("%s/%s", file.GetDatabaseFileBaseDir(databaseUuid, branchUuid), BACKUP_DIR)

	// Get a list of all directories in the directory
	dirs, err := storage.ObjectFS().ReadDir(backupsDirectory)

	if err != nil {
		log.Fatal(err)
	}

	// Loop through the directories
	for _, dir := range dirs {
		// Get the timestamp of the directory

		if !dir.IsDir {
			continue
		}

		timestamp, err := strconv.ParseInt(dir.Name, 10, 64)

		if err != nil {
			log.Fatal(err)
		}

		// If the timestamp is greater than the current timestamp, then return the backup
		backups = append(backups, timestamp)
	}

	// Sort the backups
	sort.Slice(backups, func(i, j int) bool {
		return backups[i] < backups[j]
	})

	// Loop through the backups
	for _, b := range backups {
		if b > snapshotTimestamp {
			return GetBackup(dfs, databaseUuid, branchUuid, b)
		}
	}

	return nil
}

/*
Remove the backup files from the filesystem.
*/
func (backup *Backup) Delete() error {
	hash := backup.Hash()

	// Read the directory to find matching file names and part numbers
	entries, err := storage.ObjectFS().ReadDir(backup.DirectoryPath())

	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir {
			continue
		}

		if strings.HasPrefix(entry.Name, hash) {
			if err := storage.ObjectFS().Remove(fmt.Sprintf("%s/%s", backup.DirectoryPath(), entry.Name)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (backup *Backup) DirectoryPath() string {
	return fmt.Sprintf(
		"%s/%d",
		file.GetDatabaseBackupsDirectory(backup.DatabaseUuid, backup.BranchUuid),
		backup.SnapshotTimestamp,
	)
}

/*
Returns the file path for a database backup with the given part number.
*/
func (backup *Backup) FilePath(partNumber int) string {
	return fmt.Sprintf(
		"%s/%s",
		backup.DirectoryPath(),
		backup.Key(partNumber),
	)
}

/*
Returns the maximum part size for a backup.
*/
func (backup *Backup) GetMaxPartSize() int64 {
	if backup.maxPartSize == 0 {
		return BACKUP_MAX_PART_SIZE
	}

	return backup.maxPartSize
}

/*
Returns the hash of the backup which is used to identify the backup. This hash
consists of the database UUID, branch UUID, and the snapshot timestamp.
*/
func (backup *Backup) Hash() string {
	hash := sha1.New()
	hash.Write([]byte(backup.DatabaseUuid))
	hash.Write([]byte(backup.BranchUuid))
	hash.Write([]byte(fmt.Sprintf("%d", backup.SnapshotTimestamp)))

	return hex.EncodeToString(hash.Sum(nil))
}

/*
Returns the file key for a backup part. This contains the hash of the backup
and the part number followed by the .tar.gz extension.
*/
func (backup *Backup) Key(partNumber int) string {
	return fmt.Sprintf("%s-%d.tar.gz", backup.Hash(), partNumber)
}

// TODO: How will we prevent the database from being written to while we are backing it up?
func (backup *Backup) packageBackup() error {
	sourceDirectory := file.GetDatabaseFileDir(backup.DatabaseUuid, backup.BranchUuid)

	var fileSize int64
	var partNumber = 1
	var outputFile internalStorage.File
	var tarWriter *tar.Writer
	var gzipWriter *gzip.Writer

	var err error

	// Loop through the files in the source database and copy them to the target database
	entries, err := backup.dfs.FileSystem().ReadDir(sourceDirectory)

	if err != nil {
		log.Println("Error reading source directory:", err)
		return err
	}

	for _, entry := range entries {
		if outputFile == nil {
		createFile:
			outputFile, err = backup.dfs.FileSystem().Create(backup.FilePath(partNumber))

			if err != nil {
				if os.IsNotExist(err) {
					// If the directory does not exist, create it
					if err := backup.dfs.FileSystem().MkdirAll(backup.DirectoryPath(), 0755); err != nil {
						log.Println("Error creating backup directory:", err)
						return err
					}

					goto createFile
				}

				log.Println("Error creating zip file:", err)

				return err
			}

			gzipWriter = gzip.NewWriter(outputFile)
			tarWriter = tar.NewWriter(gzipWriter)
		}

		if entry.IsDir {
			continue
		}

		// Get the full path of the source file
		path := fmt.Sprintf("%s/%s", sourceDirectory, entry.Name)

		// Open the source file
		sourceFile, err := backup.dfs.FileSystem().Open(path)

		if err != nil {
			log.Println("Error opening source file:", entry.Name, err)
			return err
		}

		// Get file info
		info, err := sourceFile.Stat()

		if err != nil {
			return err
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, info.Name())

		if err != nil {
			return err
		}

		// Write header
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		n, err := io.Copy(tarWriter, sourceFile)

		if err != nil {
			log.Println("Error copying file:", entry.Name, err)

			return err
		}

		fileSize += n

		if fileSize >= backup.GetMaxPartSize() {
			partNumber++
			fileSize = 0

			// Close the gzip writer
			if err := gzipWriter.Close(); err != nil {
				log.Println("Error closing gzip writer:", err)

				return err
			}

			// Close the tar writer
			if err := tarWriter.Close(); err != nil {
				log.Println("Error closing zip writer:", err)

				return err
			}

			// Close the file to ensure the data is flushed.
			err = outputFile.Close()

			if err != nil {
				log.Println("Error closing tar file:", err)

				return err
			}

			outputFile = nil
			tarWriter = nil
			gzipWriter = nil
		}
	}

	// Close the final tar writer
	if tarWriter != nil {
		if err := tarWriter.Close(); err != nil {
			log.Println("Error closing tar writer:", err)

			return err
		}
	}

	// Close the gzip writer
	if gzipWriter != nil {
		if err := gzipWriter.Close(); err != nil {
			log.Println("Error closing gzip writer:", err)

			return err
		}
	}

	// Close the final tar file to ensure the data is flushed.
	if outputFile != nil {
		err = outputFile.Close()

		if err != nil {
			log.Println("Error closing tar file:", err)

			return err
		}
	}

	return nil
}

func Run(
	dfs *storage.DurableDatabaseFileSystem,
	databaseUuid string,
	branchUuid string,
	callbacks ...BackupConfigCallback,
) (*Backup, error) {
	lock := GetBackupLock(file.DatabaseHash(databaseUuid, branchUuid))

	if lock.TryLock() {
		defer lock.Unlock()
	} else {
		return nil, fmt.Errorf("backup is already running")
	}

	backup := &Backup{
		dfs:               dfs,
		BranchUuid:        branchUuid,
		DatabaseUuid:      databaseUuid,
		SnapshotTimestamp: time.Now().Unix(),
	}

	for _, callback := range callbacks {
		callback(backup)
	}

	err := backup.packageBackup()

	if err != nil {
		return nil, err
	}

	return backup, nil
}

func (backup *Backup) SetMaxPartSize(size int64) {
	backup.maxPartSize = size
}

func (backup *Backup) Size() int64 {
	var size int64
	hash := backup.Hash()

	// Read the directory to find matching file names and part numbers
	entries, err := storage.ObjectFS().ReadDir(backup.DirectoryPath())

	if err != nil {
		return 0
	}

	for _, entry := range entries {
		if entry.IsDir {
			continue
		}

		if strings.HasPrefix(entry.Name, hash) {
			stat, err := storage.ObjectFS().Stat(fmt.Sprintf("%s/%s", backup.DirectoryPath(), entry.Name))

			if err != nil {
				log.Println("Error getting file size:", err)
				return 0
			}

			size += stat.Size()
		}
	}

	return size
}

func (backup *Backup) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"database_id": backup.DatabaseUuid,
		"branch_id":   backup.BranchUuid,
		"size":        backup.Size(),
		"timestamp":   backup.SnapshotTimestamp,
	}
}
