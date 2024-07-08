package database

import (
	"litebase/internal/config"
	"litebase/server/backups"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"sync"
	"time"
)

type Checkpointer struct {
	branchUuid       string
	checkpointLogger *backups.CheckpointLogger
	databaseUuid     string
	lock             sync.Mutex
	metadata         *storage.DatabaseMetadata
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
		metadata:         storage.NewDatabaseMetadata(databaseUuid, branchUuid),
		pageLogger:       backups.NewPageLogger(databaseUuid, branchUuid),
		pages:            map[uint32]bool{},
	}
}

func (c *Checkpointer) AddPage(pageNumber uint32) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.pages[pageNumber] = true
}

func (c *Checkpointer) Pages() map[uint32]bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.pages
}

func (c *Checkpointer) Run() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.running = true

	defer func() { c.running = false }()

	if len(c.pages) == 0 {
		return nil
	}

	timestamp := uint64(time.Now().Unix())
	pageSize := config.Get().PageSize
	pageCount := c.metadata.PageCount

	fs := DatabaseResources().FileSystem(c.databaseUuid, c.branchUuid)

	largestPageNumber := uint32(0)

	for pageNumber := range c.pages {
		// Read the page from the database file
		pageOffset := file.PageOffset(int64(pageNumber), pageSize)

		pageData, err := fs.ReadAt(file.DatabaseHash(c.databaseUuid, c.branchUuid), pageOffset, pageSize)

		if err != nil {
			log.Println("Error reading page", err)
			return err
		}

		err = c.pageLogger.Log(pageNumber, timestamp, pageData)

		if err != nil {
			log.Println("Error logging page", err)
			return err
		}

		if pageNumber > largestPageNumber {
			largestPageNumber = pageNumber
		}
	}

	if uint32(pageCount) < largestPageNumber {
		c.metadata.SetPageCount(int64(largestPageNumber))
	}

	err := c.checkpointLogger.Log(timestamp, uint32(pageCount))

	if err != nil {
		log.Println("Error logging checkpoint", err)
		return err
	}

	c.pages = map[uint32]bool{}

	return nil
}

func (c *Checkpointer) Running() bool {
	return c.running
}
