package backups

import (
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"fmt"
	"io"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/config"
	"litebasedb/runtime/file"
	"sort"
	"strconv"

	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Backup struct {
	databaseUuid      string
	branchUuid        string
	SnapshotTimestamp int
}

func GetBackup(databaseUuid string, branchUuid string, snapshotTimestamp time.Time) *Backup {
	backup := &Backup{
		databaseUuid:      databaseUuid,
		branchUuid:        branchUuid,
		SnapshotTimestamp: int(snapshotTimestamp.UTC().Unix()),
	}

	return backup
}

func GetNextBackup(databaseUuid string, branchUuid string, snapshotTimestamp int) *Backup {
	backups := make([]int, 0)
	backupsDirectory := fmt.Sprintf("%s/%s", file.GetFileDir(databaseUuid, branchUuid), BACKUP_DIR)

	// Get a list of all directories in the directory
	dirs, err := os.ReadDir(backupsDirectory)

	if err != nil {
		log.Fatal(err)
	}

	// Loop through the directories
	for _, dir := range dirs {
		// Get the timestamp of the directory

		if !dir.IsDir() {
			continue
		}

		timestamp, err := strconv.Atoi(dir.Name())

		if err != nil {
			log.Fatal(err)
		}

		// If the timestamp is greater than the current timestamp, then return the backup
		backups = append(backups, timestamp)
	}

	// Sort the backups
	sort.Ints(backups)

	// Loop through the backups
	for _, b := range backups {
		if b > snapshotTimestamp {
			return GetBackup(databaseUuid, branchUuid, time.Unix(int64(b), 0))
		}
	}

	return nil
}

func (backup *Backup) BackupKey() string {
	hash := sha1.New()
	hash.Write([]byte(fmt.Sprintf("%s-%s-%d", backup.databaseUuid, backup.branchUuid, backup.SnapshotTimestamp)))

	return fmt.Sprintf("%s/%d/%x.db.gz", BACKUP_DIR, backup.SnapshotTimestamp, hash.Sum(nil))
}

func (backup *Backup) Delete() {
	backup.deleteFile()
	backup.deleteArchiveFile()
}

func (backup *Backup) deleteArchiveFile() {
	if config.Get("env") == "local" {
		storageDir := file.GetFileDir(backup.databaseUuid, backup.branchUuid)
		os.Remove(fmt.Sprintf("%s/archives/%s", storageDir, backup.BackupKey()))

		return
	}

	awsCredentials, err := auth.SecretsManager().GetAwsCredentials(backup.databaseUuid, backup.branchUuid)

	if err != nil {
		log.Fatal(err)
	}

	session, err := session.NewSession(aws.NewConfig().WithRegion(config.Get("region")).WithCredentials(credentials.NewStaticCredentials(
		awsCredentials["key"],
		awsCredentials["secret"],
		awsCredentials["token"],
	)))

	if err != nil {
		log.Fatal(err)
	}

	client := s3.New(session)

	bucket, err := auth.SecretsManager().GetBackupBucketName(backup.databaseUuid, backup.branchUuid)

	if err != nil {
		log.Fatal(err)
	}

	_, err = client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(backup.BackupKey()),
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (backup *Backup) deleteFile() {
	if _, err := os.Stat(backup.Path()); os.IsNotExist(err) {
		return
	}

	os.Remove(backup.Path())
}

func (backup *Backup) packageBackup() string {
	input, err := file.GetFilePath(backup.databaseUuid, backup.branchUuid)

	if err != nil {
		panic(err)
	}

	output := backup.Path()

	file, err := os.Open(input)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	err = os.MkdirAll(filepath.Dir(output), 0755)

	if err != nil {
		panic(err)
	}

	gzipFile, err := os.Create(output)

	if err != nil {
		panic(err)
	}

	defer gzipFile.Close()
	writer := gzip.NewWriter(gzipFile)

	defer writer.Close()

	_, err = io.Copy(writer, reader)

	if err != nil {
		panic(err)
	}

	return output
}

func (backup *Backup) Path() string {
	return fmt.Sprintf("%s/%s", file.GetFileDir(backup.databaseUuid, backup.branchUuid), backup.BackupKey())
}

func RunBackup(databaseUuid string, branchUuid string) (*Backup, error) {
	backup := &Backup{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
	}

	backup.SnapshotTimestamp = int(time.Now().UTC().Unix())

	backup.packageBackup()

	return backup, nil
}

func (backup *Backup) Size() int64 {
	stat, err := os.Stat(backup.Path())

	if err != nil {
		return 0
	}

	return stat.Size()
}

func (backup *Backup) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"databaseUuid": backup.databaseUuid,
		"branchUuid":   backup.branchUuid,
		"size":         backup.Size(),
		"timestamp":    backup.SnapshotTimestamp,
	}
}

func (backup *Backup) Upload() map[string]interface{} {
	if config.Get("env") == "testing" {
		return map[string]interface{}{
			"key":  "test",
			"size": 0,
		}
	}

	path := backup.packageBackup()
	key := filepath.Base(path)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		log.Fatalf("Backup archive file not found: %s", path)
	}

	if config.Get("env") == "local" {
		storageDir := file.GetFileDir(backup.databaseUuid, backup.branchUuid)

		if _, err := os.Stat(storageDir); os.IsNotExist(err) {
			os.Mkdir(storageDir, 0755)
		}

		source, err := os.ReadFile(path)

		if err != nil {
			log.Fatal(err)
		}

		os.WriteFile(fmt.Sprintf("%s/%s", storageDir, key), source, 0644)
	} else {
		awsCredentials, err := auth.SecretsManager().GetAwsCredentials(backup.databaseUuid, backup.branchUuid)

		if err != nil {
			log.Fatal(err)
		}

		session, err := session.NewSession(aws.NewConfig().WithRegion(config.Get("region")).WithCredentials(credentials.NewStaticCredentials(
			awsCredentials["key"],
			awsCredentials["secret"],
			awsCredentials["token"],
		)))

		if err != nil {
			log.Fatal(err)
		}

		bucket, err := auth.SecretsManager().GetBackupBucketName(backup.databaseUuid, backup.branchUuid)

		if err != nil {
			log.Fatal(err)
		}

		uploader := s3manager.NewUploader(session)
		source, err := os.Open(path)

		if err != nil {
			log.Fatal(err)
		}

		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket:       aws.String(bucket),
			Key:          aws.String(key),
			Body:         source,
			ACL:          aws.String("private"),
			StorageClass: aws.String("GLACIER_IR"),
		})

		if err != nil {
			log.Fatal(err)
		}
	}

	stat, err := os.Stat(path)

	if err != nil {
		log.Fatal(err)
	}

	return map[string]interface{}{
		"key":  key,
		"size": stat.Size(),
	}
}
