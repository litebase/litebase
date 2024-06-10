package database

import (
	"litebasedb/internal/config"
	"litebasedb/server/backups"
	"litebasedb/server/file"
	"os"
	"sync"
	"time"
)

type Checkpointer struct {
	branchUuid       string
	checkpointLogger *backups.CheckpointLogger
	databaseUuid     string
	lock             sync.Mutex
	pageLogger       *backups.PageLogger
	pages            map[uint32]bool
	running          bool
}

func NewCheckpointer(databaseUuid, branchUuid string) *Checkpointer {
	return &Checkpointer{
		branchUuid:       branchUuid,
		checkpointLogger: backups.NewCheckpointLogger(databaseUuid, branchUuid),
		databaseUuid:     databaseUuid,
		lock:             sync.Mutex{},
		pageLogger:       backups.NewPageLogger(databaseUuid, branchUuid),
		pages:            map[uint32]bool{},
	}
}

func (c *Checkpointer) AddPage(pageNumber uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.pages[pageNumber] = true
}

func (c *Checkpointer) Run() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.pages) == 0 {
		return nil
	}

	c.running = true
	defer func() { c.running = false }()

	timestamp := uint64(time.Now().Unix())

	path, err := file.GetDatabaseFilePath(c.databaseUuid, c.branchUuid)

	if err != nil {
		return err
	}

	databaseFile, err := os.Open(path)

	if err != nil {
		return err
	}

	pageSize := config.Get().PageSize

	// We need to get the size of the database file to calculate the page count.
	// Normally, we could use a database connection to use the PRAGMA page_count
	// * page_size query, but we don't want to open a connection.
	fileInfo, err := os.Stat(path)

	if err != nil {
		return err
	}

	databaseSize := fileInfo.Size()
	pageCount := uint32(databaseSize / int64(pageSize))

	// TODO: Do more than one page at a time in goroutines
	for pageNumber := range c.pages {
		// Read the page from the database file
		pageOffset := file.PageOffset(int64(pageNumber), pageSize)
		_, err := databaseFile.Seek(pageOffset, 0)

		if err != nil {
			return err
		}

		pageData := make([]byte, pageSize)

		_, err = databaseFile.Read(pageData)

		if err != nil {
			return err
		}

		err = c.pageLogger.Log(pageNumber, timestamp, pageData)

		if err != nil {
			return err
		}
	}

	err = c.checkpointLogger.Log(timestamp, pageCount)

	if err != nil {
		return err
	}

	c.pages = map[uint32]bool{}

	return nil
}

func (c *Checkpointer) Running() bool {
	return c.running
}
