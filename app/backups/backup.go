package backups

import (
	"crypto/sha1"
	"fmt"
	"litebasedb/runtime/app/file"
	"log"
	"os"
	"strings"
	"time"
)

type Backup struct {
	branchUuid        string
	databaseUuid      string
	fileDirCache      map[string]bool
	fileDirectory     string
	pageHashes        []string
	snapshot          *Snapshot
	snapshotTimestamp int

	AccessHeadFile
}

func (b *Backup) createSnapShot() *Snapshot {
	snapshot := CreateSnapshot(b.databaseUuid, b.branchUuid, b.snapshotTimestamp, b.pageHashes)

	lashHash := b.getLastLineofHeadFile()
	log.Println("Writing backup manifest", snapshot.Hash, lashHash)

	if snapshot.Hash == lashHash {
		return snapshot
	}

	// Append the last hash to the head file
	headFile := b.headFilePath(b.databaseUuid, b.branchUuid, b.snapshotTimestamp)
	file, err := os.OpenFile(headFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	if _, err := file.WriteString(fmt.Sprintf("%s\n", snapshot.Hash)); err != nil {
		log.Fatal(err)
	}

	return snapshot
}

func (b *Backup) Exists() bool {
	return b.GetSnapShot() != nil
}

func (b *Backup) FileDirectory() string {
	if b.fileDirectory == "" {
		b.fileDirectory = file.GetFileDir(b.databaseUuid, b.branchUuid)
	}

	return b.fileDirectory
}

func (b *Backup) getLastLineofHeadFile() string {
	path := b.headFilePath(b.databaseUuid, b.branchUuid, b.snapshotTimestamp)
	file, err := os.Open(path)

	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}

		log.Fatal(err)
	}

	defer file.Close()

	cursor := int64(-1)
	char := make([]byte, 1)
	var line string

	file.Seek(cursor, os.SEEK_END)

	_, err = file.ReadAt(char, cursor)

	if err != nil {
		return ""
	}

	for {
		cursor--

		file.Seek(cursor, os.SEEK_END)

		_, err := file.ReadAt(char, cursor)

		if err != nil || string(char) != "\n" && string(char) != "\r" {
			break
		}
	}

	for {
		line = string(char) + line
		cursor--

		file.Seek(cursor, os.SEEK_END)
		_, err := file.ReadAt(char, cursor)

		if err != nil || string(char) == "\n" || string(char) == "\r" {
			break
		}
	}

	return string(line)
}

func (b *Backup) GetSnapShot() *Snapshot {
	return GetSnapShot(b.databaseUuid, b.branchUuid, b.snapshotTimestamp)
}

func (b *Backup) lockFilePath() string {
	return strings.Join([]string{
		b.FileDirectory(),
		BACKUP_DIR,
		"backup.lock",
	}, "/")
}

func (b *Backup) objectPath(hash string) string {
	return strings.Join([]string{
		b.FileDirectory(),
		BACKUP_DIR,
		fmt.Sprintf("%d", b.snapshotTimestamp),
		BACKUP_OBJECT_DIR,
		hash,
	}, "/")
}

func (b *Backup) ObtainLock() *Lock {
	stat, err := os.Stat(b.lockFilePath())

	if err != nil && os.IsNotExist(err) {
		return NewLock(b.lockFilePath())
	}

	fileUpdatedAt := stat.ModTime().Unix()

	if (time.Now().Unix() - fileUpdatedAt) > 180 {

		os.Remove(b.lockFilePath())
	} else {
		return nil
	}

	return NewLock(b.lockFilePath())
}

func (b *Backup) writePage(page []byte) string {
	hash := sha1.New()
	hash.Write(page)
	hashString := fmt.Sprintf("%x", hash.Sum(nil))
	fileDir := b.objectPath(hashString[0:2])
	fileName := hashString[2:]
	fileDest := strings.Join([]string{fileDir, fileName}, "/")

	if _, hasDirectoryInCache := b.fileDirCache[fileDir]; !hasDirectoryInCache {
		if _, err := os.Stat(fileDest); os.IsNotExist(err) {
			err := os.MkdirAll(fileDir, 0755)

			if err != nil {
				panic(err)
			}

			b.fileDirCache[fileDir] = true
		}
	}

	err := os.WriteFile(fileDest, page, 0644)

	if err != nil {
		log.Fatal(err)
	}

	return hashString
}

func (b *Backup) Timestamp(t time.Time) int {
	t = t.In(time.UTC)
	return int(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix())
}
