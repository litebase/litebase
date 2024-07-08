package backups

import (
	"context"
	"fmt"
	"litebase/internal/config"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
	"sync"
)

var pageSize = config.Get().PageSize

func RestoreFromTimestamp(
	databaseUuid string,
	branchUuid string,
	backupTimestamp uint64,
	onComplete func(func() error) error,
) error {
	restorePoint, err := GetRestorePoint(databaseUuid, branchUuid, backupTimestamp)

	if err != nil {
		return fmt.Errorf("restore point not found in snapshot")
	}

	// TODO: this needs to be based on split files
	// Create the new database file
	destination, err := file.GetDatabaseFilePath(databaseUuid, branchUuid)
	filePath := fmt.Sprintf("%s%s", destination, "-restore")

	if err != nil {
		return err
	}

	// TODO: this needs to be based on split files
	destinationFile, err := storage.FS().OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)

	if err != nil {
		return err
	}

	defer destinationFile.Close()

	// TOOD: Lock the databse connections from writes?

	// Walk the files in the page versions directory
	directory := file.GetDatabaseFileBaseDir(databaseUuid, branchUuid)
	path := fmt.Sprintf("%s/page_versions", directory)

	// TODO: this needs to be based on split files
	dir, err := os.Open(path)

	if err != nil {
		return err
	}

	defer dir.Close()

	errorSignal := make(chan error)

	go func() {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			for err := range errorSignal {
				fmt.Println("Error:", err)
				cancel() // cancel the context on error
			}
		}()

		var processedPages uint32 = 0
		var chunkSize uint32 = 10
		currentChunk := []uint32{1, chunkSize}

		for {
			if processedPages > restorePoint.PageCount {
				break
			}

			var wg sync.WaitGroup

			for pageNumber := currentChunk[0]; pageNumber <= currentChunk[1]; pageNumber++ {
				wg.Add(1)

				go func() {
					defer wg.Done()

					// Open the page log
					// pageLog, err := OpenPageLog(databaseUuid, branchUuid, pageNumber)

					// if err != nil {
					// 	errorSignal <- err
					// 	return
					// }

					select {
					case <-ctx.Done():
						return // exit the goroutine if the context is cancelled
					default:
						RestorePage(
							databaseUuid,
							branchUuid,
							pageNumber,
							backupTimestamp,
							destinationFile,
							errorSignal,
						)

						processedPages++
					}
				}()
			}

			wg.Wait() // wait for all goroutines in the current batch to finish

			// Update the current chunk
			currentChunk[0] = currentChunk[1] + 1
			currentChunk[1] = currentChunk[0] + chunkSize

			select {
			case <-ctx.Done():
				return // exit the loop if the context is cancelled
			default:
			}
		}

		// Close the error signal channel
		close(errorSignal)
	}()

	// Wait for all the page logs to be read
	err = <-errorSignal

	if err != nil {
		return err
	}

	// Wrap things up after running this callback
	return onComplete(func() error {
		// Rename the source file to the destination file.
		err = os.Rename(destination, destination+".bak")

		if err != nil {
			return err
		}

		err = os.Rename(filePath, destination)

		if err != nil {
			return err
		}

		// Delete the wal and shm files
		err = os.Remove(fmt.Sprintf("%s-wal", destination))

		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}

		err = os.Remove(fmt.Sprintf("%s-shm", destination))

		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}

		// Delete the backup file.
		err = os.Remove(fmt.Sprintf("%s.bak", destination))

		if err != nil {
			return err
		}

		return nil
	})
}

func RestorePage(
	databaseUuid string,
	branchUuid string,
	pageNumber uint32,
	backupTimestamp uint64,
	destinationFile internalStorage.File,
	errorSignal chan error,
) {
	// Open the page log
	pageLog, err := OpenPageLog(databaseUuid, branchUuid, pageNumber)

	if err != nil {
		errorSignal <- err
		return
	}

	defer pageLog.Close()

	// Read the page log entries
	reader, readerError := pageLog.Reader()

	for {
		select {
		case err := <-readerError:
			errorSignal <- err
			return
		case entry, ok := <-reader:
			if !ok {
				return
			}

			// TODO: We need to write the latest version of the page without having to write all previous pages first
			// Check if the entry is within the backup timestamp
			// if entry.Timestamp < backupTimestamp {
			// 	continue
			// }

			if entry.Timestamp > backupTimestamp {
				break
			}

			offset := file.PageOffset(int64(pageNumber), pageSize)

			// Write the page to the destination file
			_, err := destinationFile.WriteAt(entry.Data, offset)

			if err != nil {
				log.Println("Error writing page", pageNumber, err)
				errorSignal <- err
				return
			}
		}
	}
}
