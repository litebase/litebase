package backups

import (
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
)

/*
The PageLog is a data structure used to keep track of database page versions at
a given point in time. Each PageLog contains multiple PageLogEntries which are
used to store the page version information along with the data of the page.

In the event of a database restore, the PageLog is used to retrieve the page
version that meets the restore criteria. The PageLog is stored in a single
file on disk, and the PageLogEntries are stored in compressed frames.
*/

type PageLog struct {
	file       internalStorage.File
	PageNumber uint32
	Version    int
}

func OpenPageLog(databaseUuid, branchUuid string, pageNumber uint32) (*PageLog, error) {
log:
	directory := file.GetDatabaseFileBaseDir(databaseUuid, branchUuid)
	path := fmt.Sprintf("%s/logs/page_versions/%010d", directory, pageNumber)
	file, err := storage.TieredFS().OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = storage.TieredFS().MkdirAll(fmt.Sprintf("%s/logs/page_versions", directory), 0755)

			if err != nil {
				return nil, err
			}

			goto log
		}

		return nil, err
	}

	return &PageLog{
		file:       file,
		PageNumber: pageNumber,
	}, nil
}

func (p *PageLog) Close() {
	p.file.Close()
}

func (p *PageLog) Append(entry *PageLogEntry) error {
	p.file.Seek(0, io.SeekEnd)

	serialized, err := entry.Serialize()

	if err != nil {
		return err
	}

	_, err = p.file.Write(serialized)

	return err
}

// Read through the PageLog and return all the entries. This is done by reading
// the file from the beginning to the end and deserializing the entries.
func (p *PageLog) Reader() (chan *PageLogEntry, chan error) {
	entries := make(chan *PageLogEntry)
	readerError := make(chan error)

	go func() {
		defer close(entries)

		// Reset the file pointer to the beginning of the file
		_, err := p.file.Seek(0, io.SeekStart)

		if err != nil {
			// handle error, e.g., log it and return
			log.Println("Error seeking file:", err)
			return
		}

		for {
			// Read the next entry from the file
			pageLogEntry, err := DeserializePageLogEntry(p.file)

			if err == io.EOF {
				break
			}

			if err != nil {
				readerError <- err
				break
			}

			// Send the entry on the channel
			entries <- pageLogEntry
		}
	}()

	return entries, readerError
}
