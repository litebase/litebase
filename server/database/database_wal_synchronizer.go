package database

type DatabaseWalSynchronizer struct{}

func NewDatabaseWalSynchronizer() *DatabaseWalSynchronizer {
	return &DatabaseWalSynchronizer{}
}

func (d *DatabaseWalSynchronizer) Sync(
	databaseId, branchId string,
	data []byte,
	offset int,
	length int,
	walSha256 [32]byte,
	timestamp int64,
) error {
	// currentTimestamp, err := ConnectionManager().WalTimestamp(databaseId, branchId)

	// if err != nil {
	// 	log.Println("Error getting current timestamp", err)
	// 	return err
	// }

	// currentPath := WalVersionPath(databaseId, branchId, currentTimestamp)
	// newPath := WalVersionPath(databaseId, branchId, timestamp)

	// var newWalFileExists bool
	// fileInfo, err := storage.TmpFS().Stat(newPath)

	// if err != nil {
	// 	if !os.IsNotExist(err) {
	// 		log.Println("Error getting file info", err, newPath)
	// 		return err
	// 	}

	// 	newWalFileExists = false
	// } else {
	// 	newWalFileExists = !fileInfo.IsDir()
	// }

	// // Open the current file
	// currentWalFile, err := storage.TmpFS().OpenFile(currentPath, os.O_CREATE|os.O_RDWR, 0644)

	// if err != nil {
	// 	log.Println("Error opening file", err, currentPath)
	// 	return err
	// }

	// defer currentWalFile.Close()

	// newWalFile, err := storage.TmpFS().OpenFile(newPath, os.O_CREATE|os.O_RDWR, 0644)

	// if err != nil {
	// 	log.Println("Error creating file", err, newPath)
	// 	return err
	// }

	// defer newWalFile.Close()

	// // Copy the current file to the new file if it did not exist at the start
	// // of this operation.
	// if !newWalFileExists {
	// 	_, err = io.Copy(newWalFile, currentWalFile)

	// 	if err != nil {
	// 		log.Println("Error copying file", err, currentPath, newPath)
	// 		return err
	// 	}
	// }

	// log.Println("Syncing WAL", offset, length, len(data))
	// // Apply the changes to the new file
	// _, err = newWalFile.WriteAt(data, int64(offset))

	// if err != nil {
	// 	return err
	// }

	// // Time to check the integrity of the new file
	// newWalFile.Seek(0, 0)

	// hasher := sha256.New()

	// if _, err := newWalFile.WriteTo(hasher); err != nil {
	// 	log.Println("Error reading file", err)
	// 	return err
	// }

	// var newWalFileSha256 [32]byte

	// copy(newWalFileSha256[:], hasher.Sum(nil))

	// // If the new file is corrupt, we need to start over. We will update the WAL
	// // with an empty SHA256 hash.
	// if walSha256 != newWalFileSha256 {
	// 	log.Printf("sha256 mismatch when updating the WAL %x | %x", walSha256, newWalFileSha256)

	// 	err = ConnectionManager().UpdateWal(databaseId, branchId, [32]byte{}, timestamp)

	// 	if err != nil {
	// 		return err
	// 	}

	// 	return nil
	// }

	// err = ConnectionManager().UpdateWal(databaseId, branchId, walSha256, timestamp)

	// if err != nil {
	// 	log.Println("Error syncing WAL", err)
	// 	return err
	// }

	return nil

}

func (d *DatabaseWalSynchronizer) WalPath(databaseId, branchId string) string {
	return WalPath(databaseId, branchId)
}

// On the primary, this is the timestamp of the last transaction.
func (d *DatabaseWalSynchronizer) WalTimestamp(databaseId, branchId string) (int64, error) {
	return 0, nil
}
