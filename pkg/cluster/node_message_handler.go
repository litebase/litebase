package cluster

import (
	"crypto/sha256"
	"errors"
	"log"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/cluster/messages"
)

// Handle a message from a node in the cluster.
func (n *Node) HandleMessage(message messages.NodeMessage) (messages.NodeMessage, error) {
	var responseMessage any

	switch message := message.Data.(type) {
	case messages.HeartbeatMessage:
		responseMessage = n.handleHeartbeatMessage(message)
	case messages.NodeConnectionMessage:
		responseMessage = messages.NodeConnectionMessage{
			ID: message.ID,
		}
	case messages.QueryMessage:
		responseMessage = n.handleQueryMessage(message)
	default:
		var err error
		responseMessage, err = n.handleBroadcastMessage(message)

		if err != nil {
			responseMessage = messages.ErrorMessage{
				Message: err.Error(),
			}
		}
	}

	return messages.NodeMessage{
		Data: responseMessage,
	}, nil
}

func (n *Node) handleBroadcastMessage(message any) (any, error) {
	var responseMessage any
	var err error

	switch message := message.(type) {
	case messages.RangeReplicationTruncateMessage:
		log.Println("Received range replication truncate message")
	case messages.RangeReplicationWriteMessage:
		err = n.handleRangeReplicationWriteMessage(message)
	case messages.WALIndexHeaderMessage:
		err = n.walSynchronizer.SetWALIndexHeader(
			message.DatabaseID,
			message.BranchID,
			message.DatabaseHash,
			message.NodeHash,
			message.Timestamp,
			message.WALIndexHeader,
		)
	case messages.WALIndexTimestampMessage:
		log.Println("Received WAL index timestamp message")
		// n.walSynchronizer.SetCurrentTimestamp(
		// 	message.DatabaseID,
		// 	message.BranchID,
		// 	message.Timestamp,
		// )
	case messages.WALVersionUsageRequest:
		responseMessage, err = n.handleWALVersionUsageRequest(message)
	case messages.PageLoggerVersionUsageRequest:
		responseMessage, err = n.handlePageLoggerVersionUsageRequest(message)
	case messages.WALReplicationWriteMessage:
		err = n.handleWALReplicationWriteMessage(message)
	case messages.DatabaseBranchSettingsUpdated:
		err = n.handleDatabaseBranchSettingsUpdated(message)
	case messages.JobBatchStatusRequest:
		responseMessage, err = n.handleJobBatchStatusRequest(message)
	case messages.MigrationsUpdatedMessage:
		err = n.handleMigrationsUpdated(message)
	default:
		err = errors.New("unknown message type")
	}

	return responseMessage, err
}

func (n *Node) handleRangeReplicationWriteMessage(message messages.RangeReplicationWriteMessage) error {
	log.Println("Received range replication write message")

	// Verify the integrity of the data
	sha256Hash := sha256.Sum256(message.Data)

	if sha256Hash != message.Sha256 {
		log.Println("Failed to verify data integrity")
		return errors.New("failed to verify data integrity")
	}

	// return n.RangeSynchronizer().WriteAt(
	// 	message.DatabaseID,
	// 	message.BranchID,
	// 	message.Data,
	// 	message.Offset,
	// 	message.Sequence,
	// 	message.Timestamp,
	// )

	return nil
}

// Handle a heartbeat message from a primary or replica node.
func (n *Node) handleHeartbeatMessage(message messages.HeartbeatMessage) any {
	var responseMessage = messages.HeartbeatResponseMessage{}

	if !n.IsPrimary() {
		if message.Time > n.PrimaryHeartbeat.Unix() {
			n.mutex.Lock()
			n.PrimaryHeartbeat = time.Unix(message.Time, 0).UTC()
			n.mutex.Unlock()

			responseMessage.Time = n.PrimaryHeartbeat.Unix()
		}
	}

	return responseMessage
}

// Handle a query message from a replica node.
func (n *Node) handleQueryMessage(message messages.QueryMessage) interface{} {
	query, err := n.queryBuilder.Build(
		message.CredentialID,
		message.CredentialScheme,
		message.DatabaseID,
		message.DatabaseName,
		message.BranchID,
		message.BranchName,
		message.Statement,
		message.Parameters,
		message.ID,
	)

	if err != nil {
		log.Println("Failed to build query: ", err)

		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	response := n.queryResponsePool.Get()
	defer n.queryResponsePool.Put(response)

	// Get the wal sequence number
	// Get the wal timestamp

	response, err = query.Resolve(response)

	if err != nil {
		log.Println("Failed to process query message: ", err)
		return messages.ErrorMessage{
			Message: err.Error(),
		}
	}

	if response == nil {
		return messages.ErrorMessage{
			Message: "Failed to process query message: response is empty",
		}
	}

	return messages.QueryMessageResponse{
		Changes:         response.Changes(),
		Columns:         response.Columns(),
		Error:           response.Error(),
		ID:              message.ID,
		LastInsertRowID: response.LastInsertRowId(),
		Latency:         response.Latency(),
		RowCount:        response.RowCount(),
		Rows:            response.Rows(),
		TransactionID:   response.TransactionID(),
		WALSequence:     response.WALSequence(),
		WALTimestamp:    response.WALTimestamp(),
	}
}

func (n *Node) handleWALReplicationWriteMessage(message messages.WALReplicationWriteMessage) error {
	// Verify the integrity of the WAL data
	sha256Hash := sha256.Sum256(message.Data)

	if sha256Hash != message.Sha256 {
		log.Println("Failed to verify WAL data integrity")
		return errors.New("failed to verify WAL data integrity")
	}

	// err := n.walSynchronizer.WriteAt(
	// 	message.DatabaseID,
	// 	message.BranchID,
	// 	message.Data,
	// 	message.Offset,
	// 	message.Sequence,
	// 	message.Timestamp,
	// )

	// if err != nil {
	// 	log.Println("Failed to sync WAL data: ", err)
	// 	return err
	// }

	return nil
}

func (n *Node) handleWALVersionUsageRequest(message messages.WALVersionUsageRequest) (interface{}, error) {
	versions, err := n.walSynchronizer.GetActiveWALVersions(
		message.DatabaseID,
		message.BranchID,
	)

	if err != nil {
		log.Println("Failed to get WAL versions: ", err)
		return nil, err
	}

	return messages.WALVersionUsageResponse{
		BranchID:   message.BranchID,
		DatabaseID: message.DatabaseID,
		Versions:   versions,
	}, nil
}

// Handle a PageLoggerVersionUsageRequest message from a replica node.
func (n *Node) handlePageLoggerVersionUsageRequest(message messages.PageLoggerVersionUsageRequest) (interface{}, error) {
	// Use the page logger accessor to get in-use versions
	if n.pageLoggerAccessor == nil {
		return messages.PageLoggerVersionUsageResponse{
			BranchID:   message.BranchID,
			DatabaseID: message.DatabaseID,
			Versions:   []int64{},
		}, nil
	}

	versions, err := n.pageLoggerAccessor.GetPageLoggerInUseVersions(
		message.DatabaseID,
		message.BranchID,
	)

	if err != nil {
		slog.Error("Failed to get page logger versions", "error", err)

		return nil, err
	}

	return messages.PageLoggerVersionUsageResponse{
		BranchID:   message.BranchID,
		DatabaseID: message.DatabaseID,
		Versions:   versions,
	}, nil
}

// Handle a DatabaseBranchSettingsUpdated message to refresh branch settings from database.
func (n *Node) handleDatabaseBranchSettingsUpdated(message messages.DatabaseBranchSettingsUpdated) error {
	if n.databaseManager == nil {
		slog.Error("Database manager not set on node")
		return errors.New("database manager not configured")
	}

	// Get the database from the database manager
	db, err := n.databaseManager.Get(message.DatabaseID)

	if err != nil {
		slog.Error("Failed to get database for settings update", "error", err, "databaseId", message.DatabaseID)
		return err
	}

	// Get the branch by ID
	branch, err := db.BranchByID(message.DatabaseBranchID)

	if err != nil {
		slog.Error("Failed to get branch for settings update", "error", err, "branchId", message.DatabaseBranchID)
		return err
	}

	// Reload settings from the database
	settings, err := branch.GetBranchSettings()

	if err != nil {
		slog.Error("Failed to reload branch settings", "error", err, "branchId", message.DatabaseBranchID)
		return err
	}

	// Update the branch's settings in memory
	branch.SetSettings(settings)

	slog.Info("Branch settings updated", "databaseId", message.DatabaseID, "branchId", message.DatabaseBranchID)

	return nil
}

func (n *Node) handleJobBatchStatusRequest(message messages.JobBatchStatusRequest) (any, error) {
	if n.workerPool == nil {
		return nil, errors.New("worker pool not configured")
	}

	progress, err := n.workerPool.GetBatchStatus(n.context, message.BatchID)

	if err != nil {
		return nil, err
	}

	return messages.JobBatchStatusResponse{
		BatchID:       progress.BatchID,
		Name:          progress.Name,
		TotalJobs:     progress.TotalJobs,
		PendingJobs:   progress.PendingJobs,
		CompletedJobs: progress.CompletedJobs,
		FailedJobs:    progress.FailedJobs,
		Progress:      progress.Progress,
		IsFinished:    progress.IsFinished,
		CreatedAt:     progress.CreatedAt,
		FinishedAt:    progress.FinishedAt,
	}, nil
}

func (n *Node) handleMigrationsUpdated(message messages.MigrationsUpdatedMessage) error {
	slog.Info("Received migrations updated notification", "latest_migration", message.LatestMigration, "hash", message.MigrationsHash)
	
	// Notify the system database to recheck migrations
	if n.databaseManager != nil {
		systemDB := n.databaseManager.GetSystemDatabase()
		if systemDB != nil {
			systemDB.OnMigrationsUpdated()
		}
	}
	
	return nil
}

