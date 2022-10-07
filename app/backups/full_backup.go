package backups

import (
	"archive/zip"
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"io/fs"
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/app/database"
	"litebasedb/runtime/app/secrets"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type FullBackup struct {
	databaseUuid      string
	branchUuid        string
	snapshotTimestamp int64

	Backup
}

func (backup *FullBackup) BackupKey() string {
	hash := sha1.New()
	hash.Write([]byte(fmt.Sprintf("%s-%s-%d", backup.databaseUuid, backup.branchUuid, backup.snapshotTimestamp)))

	return fmt.Sprintf("%x.zip", hash.Sum(nil))

}

func (backup *FullBackup) Delete() {
	backup.deleteArchiveFile()
	backup.deleteDirectory()
}

func (backup *FullBackup) deleteArchiveFile() {
	if config.Get("env") == "local" {
		storageDir := fmt.Sprintf("%s/archives", filepath.Dir(backup.Directory()))
		os.Remove(fmt.Sprintf("%s/%s", storageDir, backup.BackupKey()))

		return
	}

	awsCredentials, err := secrets.Manager().GetAwsCredentials(backup.databaseUuid, backup.branchUuid)

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

	bucket := secrets.Manager().GetBackupBucketName(backup.databaseUuid, backup.branchUuid)

	_, err = client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(backup.BackupKey()),
	})

	if err != nil {
		log.Fatal(err)
	}
}

func (backup *FullBackup) deleteDirectory() {
	if _, err := os.Stat(backup.Directory()); os.IsNotExist(err) {
		return
	}

	os.RemoveAll(backup.Directory())
}

func (backup *FullBackup) Directory() string {
	return strings.Join([]string{
		database.GetFileDir(backup.databaseUuid, backup.branchUuid),
		BACKUP_DIR,
		fmt.Sprintf("%x", backup.snapshotTimestamp),
	}, "/")
}

func (backup *FullBackup) packageBackup() string {
	if _, err := os.Stat(backup.Directory()); os.IsNotExist(err) {
		log.Fatal(fmt.Sprintf("Backup directory not found: %s", backup.Directory()))
	}

	input := filepath.Dir(backup.Directory())
	output := fmt.Sprintf("%s/%s", input, backup.BackupKey())

	buf := new(bytes.Buffer)
	w := zip.NewWriter(buf)
	zipFile, err := w.Create(output)

	if err != nil {
		log.Fatal(err)
	}

	err = filepath.Walk(input, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)

		if err != nil {
			return err
		}

		defer file.Close()

		_, err = io.Copy(zipFile, file)

		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	err = w.Close()

	if err != nil {
		log.Fatal(err)
	}

	return output
}

func Run(databaseUuid string, branchUuid string) *FullBackup {
	backup := &FullBackup{
		databaseUuid:      databaseUuid,
		branchUuid:        branchUuid,
		snapshotTimestamp: time.Now().Unix(),
	}

	lock := backup.ObtainLock()

	if lock == nil {
		log.Fatal("Cannot run a full backup while another is running.")
	}

	databaseFile, err := database.NewDatabaseFile(database.GetFilePath(databaseUuid, branchUuid))

	if err != nil {
		log.Fatal(err)
	}

	defer databaseFile.Close()

	backup.pageHashes = append(backup.pageHashes, backup.writePage(databaseFile.BinaryHeader))

	i := uint32(1)

	for i < databaseFile.Header().TotalPages {
		page := databaseFile.ReadPage(int(i))
		backup.pageHashes = append(backup.pageHashes, backup.writePage(page.Data))
		i++
	}

	if len(backup.pageHashes) > 0 {
		backup.snapshot = backup.createSnapShot()
	}

	lock.Release()

	return backup
}

func (backup *FullBackup) Size() int64 {
	size := int64(0)

	filepath.Walk(backup.Directory(), func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}

		size += info.Size()

		return nil
	})

	return size
}

func (backup *FullBackup) Upload() map[string]interface{} {
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
		storageDir := fmt.Sprintf("%s/archives", filepath.Dir(backup.Directory()))

		if _, err := os.Stat(storageDir); os.IsNotExist(err) {
			os.Mkdir(storageDir, 0755)
		}

		source, err := os.ReadFile(path)

		if err != nil {
			log.Fatal(err)
		}

		os.WriteFile(fmt.Sprintf("%s/%s", storageDir, key), source, 0644)
	} else {
		awsCredentials, err := secrets.Manager().GetAwsCredentials(backup.databaseUuid, backup.branchUuid)

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

		bucket := secrets.Manager().GetBackupBucketName(backup.databaseUuid, backup.branchUuid)
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
