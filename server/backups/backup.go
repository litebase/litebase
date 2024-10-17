package backups

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"litebase/internal/config"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/storage"
	"os"
	"sort"
	"strconv"
	"strings"

	"log"
)

var BackupErrorNoRestorePoint = fmt.Errorf("no restore point found")

/*
A Backup is a complete logical snapshot of a database at a given point in time.
This data is derived from a Snapshot and can be used to restore a database.
*/
type Backup struct {
	config         *config.Config
	dfs            *storage.DurableDatabaseFileSystem
	BranchId       string
	DatabaseId     string
	maxPartSize    int64
	objectFS       *storage.FileSystem
	rollbackLogger *RollbackLogger
	RestorePoint   RestorePoint
}

type BackupConfigCallback func(backup *Backup)

/*
Returns a Backup object for the given database and branch at a timestamp.
*/
func GetBackup(
	c *config.Config,
	objectFS *storage.FileSystem,
	snapshotLogger *SnapshotLogger,
	dfs *storage.DurableDatabaseFileSystem,
	databaseId string,
	branchId string,
	timestamp int64,
) (*Backup, error) {
	snapshot, err := snapshotLogger.GetSnapshot(timestamp)

	if err != nil {
		return nil, err
	}

	restorePoint, err := snapshot.GetRestorePoint(timestamp)

	if err != nil {
		return nil, err
	}

	if restorePoint == (RestorePoint{}) {
		return nil, BackupErrorNoRestorePoint
	}

	backup := &Backup{
		config:       c,
		BranchId:     branchId,
		DatabaseId:   databaseId,
		dfs:          dfs,
		objectFS:     objectFS,
		RestorePoint: restorePoint,
	}

	return backup, nil
}

/*
Returns next backup for the given database and branch relative to the given
timestamp provided.
*/
func GetNextBackup(
	c *config.Config,
	objectFS *storage.FileSystem,
	snapshotLogger *SnapshotLogger,
	dfs *storage.DurableDatabaseFileSystem,
	databaseId string,
	branchId string,
	snapshotTimestamp int64,
) (*Backup, error) {
	backups := make([]int64, 0)
	backupsDirectory := fmt.Sprintf("%s/%s", file.GetDatabaseFileBaseDir(databaseId, branchId), BACKUP_DIR)

	// Get a list of all directories in the directory
	dirs, err := objectFS.ReadDir(backupsDirectory)

	if err != nil {
		log.Fatal(err)
	}

	// Loop through the directories
	for _, dir := range dirs {
		// Get the timestamp of the directory

		if !dir.IsDir() {
			continue
		}

		timestamp, err := strconv.ParseInt(dir.Name(), 10, 64)

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
			return GetBackup(c, objectFS, snapshotLogger, dfs, databaseId, branchId, b)
		}
	}

	return nil, fmt.Errorf("no next backup found")
}

/*
Remove the backup files from the filesystem.
*/
func (backup *Backup) Delete() error {
	hash := backup.Hash()

	// Read the directory to find matching file names and part numbers
	entries, err := backup.objectFS.ReadDir(backup.DirectoryPath())

	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.HasPrefix(entry.Name(), hash) {
			if err := backup.objectFS.Remove(fmt.Sprintf("%s/%s", backup.DirectoryPath(), entry.Name)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (backup *Backup) DirectoryPath() string {
	return fmt.Sprintf(
		"%s/%d",
		file.GetDatabaseBackupsDirectory(backup.DatabaseId, backup.BranchId),
		backup.RestorePoint.Timestamp,
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
	hash.Write([]byte(backup.DatabaseId))
	hash.Write([]byte(backup.BranchId))
	hash.Write([]byte(fmt.Sprintf("%d", backup.RestorePoint.Timestamp)))

	return hex.EncodeToString(hash.Sum(nil))
}

/*
Returns the file key for a backup part. This contains the hash of the backup
and the part number followed by the .tar.gz extension.
*/
func (backup *Backup) Key(partNumber int) string {
	return fmt.Sprintf("%s-%d.tar.gz", backup.Hash(), partNumber)
}

/*
Package the backup files into a tarball and compress it using gzip. This will
create a series of files in the filesystem that can be used to restore the
database.
*/
func (backup *Backup) packageBackup() error {
	var err error
	var fileSize int64
	var partNumber = 1
	var outputFile internalStorage.File
	var tarWriter *tar.Writer
	var gzipWriter *gzip.Writer
	var sourceFile internalStorage.File

	maxRangeNumber := file.PageRange(backup.RestorePoint.PageCount, backup.config.PageSize)
	sourceDirectory := file.GetDatabaseFileDir(backup.DatabaseId, backup.BranchId)

	// Loop through the files in the source database and copy them to the target database
	entries, err := backup.dfs.FileSystem().ReadDir(sourceDirectory)

	if err != nil {
		log.Println("Error reading source directory:", err)
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Get the full path of the source file
		path := fmt.Sprintf("%s/%s", sourceDirectory, entry.Name)

		var data []byte

		// Open the source file
		sourceFile, err = backup.dfs.FileSystem().Open(path)

		if err != nil {
			return err
		}

		// Ensure we are working on a range file
		if entry.Name()[0] != '_' {
			rangeNumber, err := strconv.ParseInt(entry.Name(), 10, 64)

			if err != nil {
				log.Println("Error parsing entry name:", entry.Name, err)
				return err
			}

			// Skip if the range number is greater than the max range number of
			// the backup based on the restore point page count.
			if rangeNumber > maxRangeNumber {
				continue
			}

			// Apply rollback logs to the file
			data, err = backup.stepApplyRollbackLogs(rangeNumber, sourceFile)

			if err != nil {
				return err
			}
		} else {
			// Currently the only other file in a database directory is the
			// metadata file. This will be the only other file that is not a
			// range file. This file needs to be updated with the page count
			// from the restore point.

			data, err = io.ReadAll(sourceFile)

			if err != nil {
				return err
			}

			if entry.Name() == "_METADATA" {
				// Set the first 8 bytes of the metadata file to the page count
				binary.LittleEndian.PutUint64(data[:8], uint64(backup.RestorePoint.PageCount))
			}
		}

		if outputFile == nil {
			outputFile, err = backup.stepCreateFile(partNumber)

			if err != nil {
				return err
			}

			// Create a new gzip and tar writer
			gzipWriter = gzip.NewWriter(outputFile)
			tarWriter = tar.NewWriter(gzipWriter)
		}

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
		header := &tar.Header{
			Name:    entry.Name(),
			ModTime: info.ModTime(),
			Mode:    int64(info.Mode()),
			Size:    int64(len(data)),
		}

		// Write header
		if err := tarWriter.WriteHeader(header); err != nil {
			log.Println("Error writing tar header:", err)
			return err
		}

		// Copy the file to the tar writer
		n, err := io.Copy(tarWriter, bytes.NewReader(data))

		if err != nil {
			log.Println("Error writing tar data:", err)

			return err
		}

		fileSize += n

		// If the file size is greater than the max part size, create a new part
		if fileSize >= backup.GetMaxPartSize() {
			partNumber++
			fileSize = 0

			// Close the tar writer
			if err := tarWriter.Close(); err != nil {
				log.Println("Error closing zip writer:", err)

				return err
			}

			// Close the gzip writer
			if err := gzipWriter.Close(); err != nil {
				log.Println("Error closing gzip writer:", err)

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
		if err := outputFile.Close(); err != nil {
			log.Println("Error closing tar file:", err)

			return err
		}
	}

	return nil
}

/*
Run a backup for the given database and branch. This will create a snapshot of
the database and store it in the filesystem. The backup will be based on the
current state of the database at the time of backup. As the backup runs,
rollback logs will be applied where needed to keep the database in the
propert state. This will allow the backup to copy all existing files
while the database is online and in use.
*/
func Run(
	databaseId string,
	branchId string,
	timestamp int64,
	snapshotLogger *SnapshotLogger,
	dfs *storage.DurableDatabaseFileSystem,
	rollbackLogger *RollbackLogger,
	callbacks ...BackupConfigCallback,
) (*Backup, error) {
	lock := GetBackupLock(file.DatabaseHash(databaseId, branchId))

	if lock.TryLock() {
		defer lock.Unlock()
	} else {
		return nil, fmt.Errorf("backup is already running")
	}

	snapshot, err := snapshotLogger.GetSnapshot(timestamp)

	if err != nil {
		return nil, err
	}

	restorePoint, err := snapshot.GetRestorePoint(timestamp)

	if err != nil {
		return nil, err
	}

	if restorePoint == (RestorePoint{}) {
		return nil, BackupErrorNoRestorePoint
	}

	backup := &Backup{
		BranchId:       branchId,
		DatabaseId:     databaseId,
		dfs:            dfs,
		RestorePoint:   restorePoint,
		rollbackLogger: rollbackLogger,
	}

	for _, callback := range callbacks {
		callback(backup)
	}

	err = backup.packageBackup()

	if err != nil {
		return nil, err
	}

	return backup, nil
}

/*
Set the maximum part size for a backup. This is the maximum size of each part
of the backup. If the backup exceeds this size, then it will be split into
multiple parts.
*/
func (backup *Backup) SetMaxPartSize(size int64) {
	backup.maxPartSize = size
}

/*
Returns the size of the backup in bytes.
*/
func (backup *Backup) Size() int64 {
	var size int64
	hash := backup.Hash()

	// Read the directory to find matching file names and part numbers
	entries, err := backup.objectFS.ReadDir(backup.DirectoryPath())

	if err != nil {
		return 0
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if strings.HasPrefix(entry.Name(), hash) {
			stat, err := backup.objectFS.Stat(fmt.Sprintf("%s/%s", backup.DirectoryPath(), entry.Name))

			if err != nil {
				log.Println("Error getting file size:", err)
				return 0
			}

			size += stat.Size()
		}
	}

	return size
}

func (backup *Backup) stepApplyRollbackLogs(rangeNumber int64, sourceFile internalStorage.File) ([]byte, error) {
	return ReadBackupRangeFile(
		backup.config,
		sourceFile,
		rangeNumber,
		backup.RestorePoint,
		backup.rollbackLogger,
	)
}

/*
Create a new file for the backup part.
*/
func (backup *Backup) stepCreateFile(partNumber int) (outputFile internalStorage.File, err error) {
createFile:
	outputFile, err = backup.dfs.FileSystem().Create(backup.FilePath(partNumber))

	if err != nil {
		if os.IsNotExist(err) {
			// If the directory does not exist, create it
			if err := backup.dfs.FileSystem().MkdirAll(backup.DirectoryPath(), 0755); err != nil {
				log.Println("Error creating backup directory:", err)
				return nil, err
			}

			goto createFile
		}

		log.Println("Error creating output file:", err)

		return nil, err
	}

	return outputFile, nil
}

/*
Returns a map representation of the backup.
*/
func (backup *Backup) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"database_id": backup.DatabaseId,
		"branch_id":   backup.BranchId,
		"size":        backup.Size(),
		"timestamp":   backup.RestorePoint.Timestamp,
	}
}
