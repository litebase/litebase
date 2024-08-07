package backups

import (
	"log"
	"sync"
)

// TODO: Add a LFU cache for page logs to reduce the number of files that are
// opened and closed
type PageLogger struct {
	databaseUuid string
	branchUuid   string
	mutext       *sync.Mutex
}

func NewPageLogger(databaseUuid, branchUuid string) *PageLogger {
	return &PageLogger{
		databaseUuid: databaseUuid,
		branchUuid:   branchUuid,
		mutext:       &sync.Mutex{},
	}
}

func (p *PageLogger) Close() {
}

func (p *PageLogger) Log(pageNumber uint32, timstamp uint64, data []byte) error {
	pageLog, err := OpenPageLog(p.databaseUuid, p.branchUuid, pageNumber)

	if err != nil {
		log.Println("Error opening page log", err)
		return err
	}

	return pageLog.Append(NewPageLogEntry(pageNumber, timstamp, data))
}
