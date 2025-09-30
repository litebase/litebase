package storage

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/file"
)

const (
	DefaultPageLoggerCompactInterval = time.Hour
	PageLoggerMaxPages               = 4294967295
	PageLoggerPageGroups             = 4096
)

var (
	PageLoggerCompactInterval      = DefaultPageLoggerCompactInterval
	pageLoggerCompactIntervalMutex sync.RWMutex

	ErrCompactionInProgress = errors.New("compaction already in progress")
)

// GetPageLoggerCompactInterval returns the current page logger compact interval safely
func GetPageLoggerCompactInterval() time.Duration {
	pageLoggerCompactIntervalMutex.RLock()
	defer pageLoggerCompactIntervalMutex.RUnlock()
	return PageLoggerCompactInterval
}

// SetPageLoggerCompactInterval sets the page logger compact interval safely
func SetPageLoggerCompactInterval(interval time.Duration) {
	pageLoggerCompactIntervalMutex.Lock()
	defer pageLoggerCompactIntervalMutex.Unlock()
	PageLoggerCompactInterval = interval
}

type PageGroup int64
type PageGroupVersion int64
type PageNumber int64
type PageVersion int64

type PageLogger struct {
	BranchID        string
	CompactedAt     time.Time
	compactionMutex *sync.Mutex
	DatabaseID      string
	NetworkFS       *FileSystem
	index           *PageLoggerIndex
	logs            map[PageGroup]map[PageGroupVersion]*PageLog
	logUsage        map[int64]int64
	mutex           *sync.Mutex
	nodePublisher   NodePublisher
	writtenAt       time.Time
}

type PageLogEntry struct {
	pageGroup        PageGroup
	pageGroupVersion PageGroupVersion
	pageLog          *PageLog
}

// Create a new instance of a page logger for the given database and branch.
func NewPageLogger(
	databaseId string,
	branchId string,
	networkFS *FileSystem,
	nodePublisher NodePublisher,
) (*PageLogger, error) {
	path := file.GetDatabaseFileBaseDir(databaseId, branchId)
	pli, err := NewPageLoggerIndex(networkFS, fmt.Sprintf("%slogs/page/PAGE_LOGGER_INDEX", path))

	if err != nil {
		return nil, err
	}

	pl := &PageLogger{
		BranchID:        branchId,
		compactionMutex: &sync.Mutex{},
		DatabaseID:      databaseId,
		NetworkFS:       networkFS,
		index:           pli,
		logs:            make(map[PageGroup]map[PageGroupVersion]*PageLog),
		logUsage:        make(map[int64]int64),
		mutex:           &sync.Mutex{},
		nodePublisher:   nodePublisher,
	}

	err = pl.load()

	if err != nil {
		log.Println("Error loading page logger:", err)
		return nil, err
	}

	// Load last compaction time from the index
	pl.CompactedAt = pl.index.GetLastCompactionAt()

	return pl, nil
}

// Acquire a reference to a specific timestamp in the page logger. This is used to
// prevent compaction of logs that are in use. Each call to Acquire must be matched
// with a call to Release.
func (pl *PageLogger) Acquire(timestamp int64) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	pl.logUsage[timestamp] = pl.logUsage[timestamp] + 1
}

// Close the page logger and all its associated page logs. This will flush any
// pending writes to disk and close all open file handles.
func (pl *PageLogger) Close() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	if pl.index != nil {
		err := pl.index.Close()

		if err != nil {
			return err
		}
	}

	for _, group := range pl.logs {
		for _, log := range group {
			err := log.Close()

			if err != nil {
				return err
			}
		}
	}

	pl.logs = make(map[PageGroup]map[PageGroupVersion]*PageLog)

	return nil
}

// Compact the page logger. This will compact all page logs that are not in use
// and remove them from the page logger index. The compaction will only run if
// the compaction interval has passed since the last compaction.
func (pl *PageLogger) Compact(
	durableDatabaseFileSystem *DurableDatabaseFileSystem,
) error {
	return pl.CompactionBarrier(func() error {
		return pl.compactInternal(durableDatabaseFileSystem)
	})
}

// compactInternal performs the actual compaction without acquiring the
// compaction barrier. This should only be called when the compaction barrier
// is already held.
func (pl *PageLogger) compactInternal(
	durableDatabaseFileSystem *DurableDatabaseFileSystem,
) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	compactInterval := GetPageLoggerCompactInterval()

	if compactInterval != 0 && !pl.CompactedAt.IsZero() && pl.CompactedAt.After(time.Now().UTC().Add(-compactInterval)) {
		return nil
	}

	if pl.writtenAt.IsZero() || pl.writtenAt.Before(pl.CompactedAt) {
		return nil
	}

	compactionErr := pl.compaction(durableDatabaseFileSystem)

	if compactionErr != nil {
		slog.Error("Error during page logger compaction", "error", compactionErr)

		return compactionErr
	}

	return nil
}

// Run the page logger compaction process.
func (pl *PageLogger) compaction(durableDatabaseFileSystem *DurableDatabaseFileSystem) error {
	err := pl.reload()

	if err != nil {
		slog.Error("Error reloading page logger for compaction", "error", err)
		return err
	}

	if len(pl.logs) == 0 {
		return nil
	}

	// Get non-empty page logs for regular compaction
	pageLogs := pl.getPageLogsForCompaction()

	// Compact non-empty logs
	for _, logEntry := range pageLogs {
		// Skip empty logs during regular compaction
		if logEntry.pageLog.index.Empty() {
			continue
		}

		rangeNumber := int64(logEntry.pageGroup)

		err := logEntry.pageLog.compact(durableDatabaseFileSystem, rangeNumber)

		if err != nil {
			slog.Error("Error compacting page log:", "error", err)
		}
	}

	// Combine non-empty compacted logs and empty logs for deletion
	allLogsToDelete := make([]PageLogEntry, 0, len(pageLogs))

	// Add non-empty logs that were compacted
	for _, logEntry := range pageLogs {
		if !logEntry.pageLog.index.Empty() {
			allLogsToDelete = append(allLogsToDelete, logEntry)
		}
	}

	if len(allLogsToDelete) == 0 {
		return nil
	}

	// Delete all logs (both compacted and empty)
	for _, logEntry := range allLogsToDelete {
		err := logEntry.pageLog.Delete()

		if err != nil {
			log.Println("Error deleting page log:", err)
		}

		delete(pl.logs[logEntry.pageGroup], logEntry.pageGroupVersion)

		if len(pl.logs[logEntry.pageGroup]) == 0 {
			delete(pl.logs, logEntry.pageGroup)
		}
	}

	err = pl.index.removePageLogs(allLogsToDelete)

	if err != nil {
		return err
	}

	pl.CompactedAt = time.Now().UTC()
	pl.index.boundary = PageGroupVersion(pl.CompactedAt.UnixNano())

	// Persist the compaction timestamp to the index
	err = pl.index.SetLastCompactionAt(pl.CompactedAt)

	if err != nil {
		slog.Error("Failed to persist last compaction timestamp", "error", err)
	}

	return nil
}

// Create a barrier for compaction operations. This ensures that only one
// compaction operation can run at a time.
func (pl *PageLogger) CompactionBarrier(f func() error) error {
	if !pl.compactionMutex.TryLock() {
		return ErrCompactionInProgress
	}

	defer pl.compactionMutex.Unlock()

	return f()
}

// Create a passive barrier for compaction operations. This ensures that only one
// compaction operation can run at a time.
func (pl *PageLogger) CompactionPassiveBarrier(f func() error) error {
	pl.compactionMutex.Lock()
	defer pl.compactionMutex.Unlock()

	return f()
}

// Create a new instance of a page log for the given log group and timestamp.
func (pl *PageLogger) createNewPageLog(logGroup PageGroup, logTimestamp PageGroupVersion) (*PageLog, error) {
	return NewPageLog(
		pl.NetworkFS,
		fmt.Sprintf(
			"%slogs/page/PAGE_LOG_%d_%d",
			file.GetDatabaseFileBaseDir(pl.DatabaseID, pl.BranchID),
			logGroup,
			logTimestamp,
		),
	)
}

// Force compaction of the page logger. This is used to ensure that the
// page logger is compacted immediately, regardless of the compaction interval.
func (pl *PageLogger) ForceCompact(
	durableDatabaseFileSystem *DurableDatabaseFileSystem,
) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	return pl.CompactionBarrier(func() error {
		err := pl.compaction(durableDatabaseFileSystem)

		if err != nil {
			slog.Error("Error during forced page logger compaction", "error", err)
			return err
		}

		return nil
	})
}

// Get the log group number for a given page number. This is used to determine
// which page log a page belongs to. Page logs are sharded by groups of pages to
// limit the number of open file handles and improve performance.
func (pl *PageLogger) getLogGroupNumber(pageNumber int64) int64 {
	return (pageNumber-1)/PageLoggerPageGroups + 1
}

// Get a list of page logs that are eligible for compaction. This will return
// all page logs that are not currently in use. A page log is considered in use
// if it has been acquired by a caller using the Acquire method.
func (pl *PageLogger) getPageLogsForCompaction() []PageLogEntry {
	pageLogs := make([]PageLogEntry, 0)

	// Get the lowest timestamp in log usage (local transactions)
	lowestLocalTimestamp := int64(0)

	for timestamp := range pl.logUsage {
		if lowestLocalTimestamp == 0 || timestamp < lowestLocalTimestamp {
			lowestLocalTimestamp = timestamp
		}
	}

	// Get oldest timestamp from replicas to ensure consistency across the cluster
	oldestReplicaTimestamp := pl.getOldestReplicaTimestamp()

	// Use the earlier of local lowest timestamp or replica timestamp to be safe
	// If there are no local transactions, we still need to respect replica constraints
	effectiveOldestTimestamp := lowestLocalTimestamp

	if oldestReplicaTimestamp != 0 && (effectiveOldestTimestamp == 0 || oldestReplicaTimestamp < effectiveOldestTimestamp) {
		effectiveOldestTimestamp = oldestReplicaTimestamp
	}

	for pageGroup, group := range pl.logs {
		for pageGroupVersion, pageLog := range group {
			// Skip if this version is still in use locally or by replicas
			if effectiveOldestTimestamp != 0 && pageGroupVersion >= PageGroupVersion(effectiveOldestTimestamp) {
				continue
			}

			pageLogs = append(pageLogs, PageLogEntry{
				pageGroup:        pageGroup,
				pageGroupVersion: pageGroupVersion,
				pageLog:          pageLog,
			})
		}
	}

	if len(pageLogs) == 0 {
		return nil
	}

	slices.SortFunc(pageLogs, func(a, b PageLogEntry) int {
		if a.pageGroupVersion < b.pageGroupVersion {
			return -1
		} else if a.pageGroupVersion > b.pageGroupVersion {
			return 1
		}

		return 0
	})

	return pageLogs
}

// getOldestReplicaTimestamp queries replicas for their oldest timestamps to ensure
// safe compaction across the distributed system
func (pl *PageLogger) getOldestReplicaTimestamp() int64 {
	// Skip replica coordination for replica nodes
	if pl.nodePublisher == nil || pl.nodePublisher.IsReplica() {
		return 0
	}

	// Create message using our internal types to avoid circular dependencies
	message := messages.PageLoggerVersionUsageRequest{
		BranchID:   pl.BranchID,
		DatabaseID: pl.DatabaseID,
	}

	responseMap, errorMap := pl.nodePublisher.Publish(message)

	// Log errors but don't fail the operation
	for _, err := range errorMap {
		if err != nil {
			slog.Error("failed to get page logger version usage from replica", "error", err)
		}
	}

	// Find the oldest timestamp from all replica responses
	var oldestTimestamp int64

	for _, response := range responseMap {
		// Skip nil responses
		if response == nil {
			continue
		}

		// The NodeAdapter should have already unwrapped the NodeMessage, so we expect
		// the response to be the actual PageLoggerVersionUsageResponse
		if responseData, ok := response.(messages.PageLoggerVersionUsageResponse); ok {
			for _, version := range responseData.Versions {
				if oldestTimestamp == 0 || version < oldestTimestamp {
					oldestTimestamp = version
				}
			}
		}
	}

	return oldestTimestamp
}

// GetInUseVersions returns all timestamps currently in use by local transactions.
// This is used to respond to requests from primary nodes during distributed compaction.
func (pl *PageLogger) GetInUseVersions() []int64 {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	var versions []int64

	for timestamp, usage := range pl.logUsage {
		if usage > 0 {
			versions = append(versions, timestamp)
		}
	}

	return versions
}

// Load the page logger index and all associated page logs. This is called when
// the page logger is created to initialize its state from disk. It will scan
// the log directory for all page log files and load their metadata.
func (pl *PageLogger) load() error {
	// Reinitialize the logs map
	pl.logs = make(map[PageGroup]map[PageGroupVersion]*PageLog)

	// Scan the log directory
	logDir := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(pl.DatabaseID, pl.BranchID))

	files, err := pl.NetworkFS.ReadDir(logDir)

	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Ensure file name starts with PAGE_LOG_*_*
		if !strings.HasPrefix(file.Name(), "PAGE_LOG_") {
			continue
		}

		parts := strings.Split(file.Name(), "_")

		// Skip files that don't match the expected pattern
		if len(parts) < 4 {
			slog.Warn("Skipping file with unexpected name format:", "file", file.Name())
			continue
		}

		logGroupNumber, err := strconv.ParseInt(parts[2], 10, 64)

		if err != nil {
			slog.Error("Error parsing log group number from file:", "file", file.Name(), "error", err)
			return err
		}

		versionNumber, err := strconv.ParseInt(parts[3], 10, 64)

		if err != nil {
			slog.Error("Error parsing version number from file:", "file", file.Name(), "error", err)
			return err
		}

		pageLog, err := pl.createNewPageLog(PageGroup(logGroupNumber), PageGroupVersion(versionNumber))

		if err != nil {
			slog.Error("Error creating new page log:", "error", err)

			return err
		}

		if pl.logs[PageGroup(logGroupNumber)] == nil {
			pl.logs[PageGroup(logGroupNumber)] = make(map[PageGroupVersion]*PageLog)
		}

		pl.logs[PageGroup(logGroupNumber)][PageGroupVersion(versionNumber)] = pageLog
	}

	return nil
}

// Read a page from the page logger. This will search through all available page
// logs for the given page number and timestamp. It will return the latest
// version of the page that is less than or equal to the given timestamp.
// If no page is found, it will return false.
func (pl *PageLogger) Read(
	pageNumber int64,
	timestamp int64,
	data []byte,
) (bool, PageVersion, error) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	pageGroupVersions := make([]PageGroupVersion, 0)
	pageGroup := pl.getLogGroupNumber(pageNumber)

	for version := range pl.logs[PageGroup(pageGroup)] {
		pageGroupVersions = append(pageGroupVersions, version)
	}

	slices.Sort(pageGroupVersions)

	// Search the all available page group versions in reverse order to find
	// the latest version of a page.
	for i := len(pageGroupVersions) - 1; i >= 0; i-- {
		pageGroupVersion := pageGroupVersions[i]

		if pageGroupVersion > PageGroupVersion(timestamp) {
			continue
		}

		logGroup := pl.getLogGroupNumber(pageNumber)

		if pl.logs[PageGroup(logGroup)] == nil {
			pl.logs[PageGroup(logGroup)] = make(map[PageGroupVersion]*PageLog)
		}

		// Check if the page log already exists, if not create it
		if pl.logs[PageGroup(logGroup)][pageGroupVersion] == nil {
			pLog, err := pl.createNewPageLog(
				PageGroup(logGroup),
				pageGroupVersion,
			)

			if err != nil {
				log.Println("Error creating page log", err)
				return false, 0, err
			}

			pl.logs[PageGroup(logGroup)][pageGroupVersion] = pLog
		}

		// Read the data from the page log if available
		pLog, ok := pl.logs[PageGroup(logGroup)][pageGroupVersion]

		if !ok {
			continue
		}

		found, foundVersion, err := pLog.Get(PageNumber(pageNumber), PageVersion(timestamp), data)

		if err != nil {
			return false, 0, err
		}

		if found {
			return true, foundVersion, nil
		}
	}

	return false, 0, nil
}

// Release a reference to a specific timestamp in the page logger. This will
// decrement the usage count for the given timestamp. If the usage count
// reaches zero, the timestamp will be removed from the usage map. This
// indicates that the page log is no longer in use and can be safely compacted.
func (pl *PageLogger) Release(timestamp int64) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	pl.logUsage[timestamp] = pl.logUsage[timestamp] - 1

	if pl.logUsage[timestamp] <= 0 {
		delete(pl.logUsage, timestamp)
	}
}

// Reload the page logger index and logs to ensure the view of all page logs is
// up to date. This is useful when the page logger is used in a distributed
// environment and the logs may have changed due to compaction or before
// performing a checkpoint operation.
func (pl *PageLogger) reload() error {
	// Close all existing logs
	for _, group := range pl.logs {
		for _, log := range group {
			err := log.Close()

			if err != nil {
				return err
			}
		}
	}

	pl.logs = make(map[PageGroup]map[PageGroupVersion]*PageLog)

	// Reload the index
	err := pl.index.load()

	if err != nil {
		log.Println("Error reloading page logger index:", err)
		// Reset the index state if loading fails
		pl.index.pageGroups = make(map[PageGroup]map[PageGroupVersion][]PageNumber)
		pl.index.boundary = PageGroupVersion(0)
	}

	return pl.load()
}

// Sync all page logs and the page logger index to ensure all data is flushed to
// disk. This should be called after a checkpoint is committed to ensure that
// all data is persisted.
func (pl *PageLogger) Sync() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	for _, group := range pl.logs {
		for _, log := range group {
			err := log.Sync()

			if err != nil {
				slog.Warn("Error syncing page log", "error", err)
				return err
			}
		}
	}

	return nil
}

// Tombstone a specific timestamp in all page logs. This will mark the given
// timestamp as deleted in all page logs. This is used to indicate that all
// pages with the given timestamp are no longer valid and should not be
// returned by reads. This is useful for discarding data during a rollback.
func (pl *PageLogger) Tombstone(timestamp int64) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	pageGroupVersions := pl.index.getPageGroupByTimestamp(PageVersion(timestamp))

	for _, pageGroupVersion := range pageGroupVersions {
		pLog := pl.logs[pageGroupVersion.pageGroup][pageGroupVersion.pageGroupVersion]

		err := pLog.Tombstone(PageVersion(timestamp))

		if err != nil {
			return err
		}
	}

	return nil
}

// Tombstone all timestamps after a specific timestamp in all page logs. This
// operation may need to be performed when copying a set of page logs that are
// part of a restore.
func (pl *PageLogger) TombstoneAfter(timestamp int64) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	// Iterate through all page groups and their versions
	for _, groupVersions := range pl.logs {
		for _, pageLog := range groupVersions {
			// For each page log, tombstone entries that are after the timestamp
			// The pageGroupVersion is just the organizing timestamp for the log file,
			// but individual entries within the log can have different timestamps
			err := pageLog.TombstoneAfterTimestamp(PageVersion(timestamp))

			if err != nil {
				slog.Error("Error tombstoning entries after timestamp in page log", "error", err)
				return err
			}
		}
	}

	return nil
}

// Write data to the appropriate page log. Page logs are distributed into shards
// based on the page number and segmented by timestamp.
func (pl *PageLogger) Write(
	page int64,
	timestamp int64,
	data []byte,
) (int, error) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	logGroup := pl.getLogGroupNumber(page)

	logTimestamp, _, err := pl.index.Find(PageGroup(logGroup), PageNumber(page), PageVersion(timestamp))

	if err != nil {
		return 0, err
	}

	if logTimestamp == 0 {
		logTimestamp = timestamp
	}

	// Ensure the page log group exists
	if pl.logs[PageGroup(logGroup)] == nil {
		pl.logs[PageGroup(logGroup)] = make(map[PageGroupVersion]*PageLog)
	}

	// Ensure the specific page log exists for this timestamp
	if pl.logs[PageGroup(logGroup)][PageGroupVersion(logTimestamp)] == nil {
		pLog, err := pl.createNewPageLog(
			PageGroup(logGroup),
			PageGroupVersion(logTimestamp),
		)

		if err != nil {
			log.Println("Error creating page log", err)
			return 0, err
		}

		pl.logs[PageGroup(logGroup)][PageGroupVersion(logTimestamp)] = pLog
	}

	err = pl.index.Push(PageGroup(logGroup), PageNumber(page), PageGroupVersion(logTimestamp))

	if err != nil {
		log.Println("Error pushing page log index", err)
		return 0, err
	}

	p, ok := pl.logs[PageGroup(logGroup)][PageGroupVersion(logTimestamp)]

	if !ok {
		log.Println("Page log not found for timestamp", logTimestamp)
		return 0, fmt.Errorf("page log not found for timestamp %d", logTimestamp)
	}

	err = p.Append(page, timestamp, data)

	if err != nil {
		log.Println("Error appending page log", err)
		return 0, err
	}

	pl.writtenAt = time.Now().UTC()

	return len(data), nil
}
