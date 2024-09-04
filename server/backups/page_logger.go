package backups

import (
	"bytes"
	"log"
	"sync"
)

// TODO: Add a LFU cache for page logs to reduce the number of files that are
// opened and closed
type PageLogger struct {
	buffers      sync.Pool
	databaseUuid string
	branchUuid   string
	mutex        *sync.Mutex
}

func NewPageLogger(databaseUuid, branchUuid string) *PageLogger {
	return &PageLogger{
		buffers: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1024))
			},
		},
		databaseUuid: databaseUuid,
		branchUuid:   branchUuid,
		mutex:        &sync.Mutex{},
	}
}

func (p *PageLogger) Close() {
}

func (p *PageLogger) Log(pageNumber uint32, timstamp uint64, data []byte) error {
	compressionBuffer := p.buffers.Get().(*bytes.Buffer)
	defer p.buffers.Put(compressionBuffer)

	compressionBuffer.Reset()

	pageLog, err := OpenPageLog(p.databaseUuid, p.branchUuid, pageNumber)

	if err != nil {
		log.Println("Error opening page log", err)
		return err
	}

	return pageLog.Append(
		compressionBuffer,
		NewPageLogEntry(pageNumber, timstamp, data),
	)
}
