package backups

import (
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"fmt"
	"io"
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/app/file"

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

type FullBackup struct {
	databaseUuid      string
	branchUuid        string
	snapshotTimestamp int
}

func GetFullBackup(databaseUuid string, branchUuid string, snapshotTimestamp time.Time) *FullBackup {
	backup := &FullBackup{
		databaseUuid:      databaseUuid,
		branchUuid:        branchUuid,
		snapshotTimestamp: int(snapshotTimestamp.UTC().Unix()),
	}

	return backup
}

func (backup *FullBackup) BackupKey() string {
	hash := sha1.New()
	hash.Write([]byte(fmt.Sprintf("%s-%s-%d", backup.databaseUuid, backup.branchUuid, backup.snapshotTimestamp)))

	return fmt.Sprintf("%x.db.gz", hash.Sum(nil))
}

func (backup *FullBackup) Delete() {
	backup.deleteArchiveFile()
}

func (backup *FullBackup) deleteArchiveFile() {
	if config.Get("env") == "local" {
		storageDir := file.GetFileDir(backup.databaseUuid, backup.branchUuid)
		os.Remove(fmt.Sprintf("%s/%s", storageDir, backup.BackupKey()))

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

// func (backup *FullBackup) deleteDirectory() {
// 	if _, err := os.Stat(backup.Directory()); os.IsNotExist(err) {
// 		return
// 	}

// 	os.RemoveAll(backup.Directory())
// }

func (backup *FullBackup) packageBackup() string {
	input, err := file.GetFilePath(backup.databaseUuid, backup.branchUuid)

	if err != nil {
		panic(err)
	}

	output := fmt.Sprintf("%s/%s", file.GetFileDir(backup.databaseUuid, backup.branchUuid), backup.BackupKey())

	file, err := os.Open(input)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	// io.ReadAll(reader)

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

func RunFullBackup(databaseUuid string, branchUuid string) (*FullBackup, error) {
	backup := &FullBackup{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
	}

	backup.snapshotTimestamp = int(time.Now().UTC().Unix())

	backup.packageBackup()

	return backup, nil
}

func (backup *FullBackup) Size() int64 {
	storageDir := file.GetFileDir(backup.databaseUuid, backup.branchUuid)
	path := fmt.Sprintf("%s/%s", storageDir, backup.BackupKey())

	stat, err := os.Stat(path)

	if err != nil {
		return 0
	}

	return stat.Size()
}

func (backup *FullBackup) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"databaseUuid": backup.databaseUuid,
		"branchUuid":   backup.branchUuid,
		"size":         backup.Size(),
		"timestamp":    backup.snapshotTimestamp,
	}
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
