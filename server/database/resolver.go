package database

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/server/logs"
	"github.com/litebase/litebase/server/sqlite3"
)

func ResolveQuery(logManager *logs.LogManager, query *Query, response *QueryResponse) (*QueryResponse, error) {
	if query.invalid {
		return nil, fmt.Errorf("invalid or malformed query")
	}

	// Determine if the query should be forwarded to the primary node.
	if shouldForwardToPrimary(query) {
		// TODO: There is an issue where if a node has been inactive and a query
		// is being resolved before election that the single node may try to
		// communicate to a non-existent primary node. This should be fixed and tested.
		// Create primary server and replica, write to replica, stop primary
		return forwardQueryToPrimary(query, response)
	}

	return resolveQueryLocally(logManager, query, response)
}

func resolveQueryLocally(logManager *logs.LogManager, query *Query, response *QueryResponse) (*QueryResponse, error) {
	return resolveWithQueue(query, response, func(query *Query, response *QueryResponse) (*QueryResponse, error) {
		start := time.Now().UTC()
		var sqlite3Result *sqlite3.Result
		var statement Statement
		var changes int64
		var lastInsertRowID int64
		var err error
		var db *ClientConnection
		var transaction *Transaction

		if query.IsTransactionStart() {
			// Handle transaction begin
			transaction, err = query.databaseManager.Resources(
				query.DatabaseKey.DatabaseId,
				query.DatabaseKey.BranchId,
			).TransactionManager().Create(
				query.cluster,
				query.databaseManager,
				query.DatabaseKey,
				query.AccessKey,
			)
		} else if query.IsTransactionEnd() {
			// Handle transaction end
			transaction, err = query.databaseManager.Resources(
				query.DatabaseKey.DatabaseId,
				query.DatabaseKey.BranchId,
			).TransactionManager().Get(string(query.Input.TransactionId))

			if err != nil {
				return nil, err
			}

			err = transaction.Commit()
		} else if query.IsTransactionRollback() {
			// Handle transaction rollback
			transaction, err = query.databaseManager.Resources(
				query.DatabaseKey.DatabaseId,
				query.DatabaseKey.BranchId,
			).TransactionManager().Get(string(query.Input.TransactionId))

			if err != nil {
				return nil, err
			}

			err = transaction.Rollback()
		} else if !query.IsTransactional() {
			// Handle non-transactional queries
			db, err = query.databaseManager.ConnectionManager().Get(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId)

			if err != nil {
				log.Println("Error getting database connection", err)
				response.SetError(err.Error())

				return response, err
			}

			defer query.databaseManager.ConnectionManager().Release(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId, db)
		} else {
			// Handle transactional queries
			db = query.transaction.connection
		}

		if db != nil {
			db = db.WithAccessKey(query.AccessKey)
		}

		if !query.IsTransactionStart() && !query.IsTransactionEnd() && !query.IsTransactionRollback() {
			if query.IsPragma() {
				sqlite3Result, err = db.GetConnection().SqliteConnection().Exec(db.GetConnection().Context(), query.Input.Statement)
				changes = db.GetConnection().Changes()
			} else {
				statement, err = db.GetConnection().Statement(query.Input.Statement)

				if err == nil {
					sqlite3Result = db.GetConnection().ResultPool().Get()
					defer db.GetConnection().ResultPool().Put(sqlite3Result)

					sqlite3Result.Reset()

					if !query.IsTransactional() {
						err = db.GetConnection().Query(
							sqlite3Result,
							statement.Sqlite3Statement,
							query.Input.Parameters,
						)
					} else {
						// TODO: Does this need the checkpointer boundary?
						err = statement.Sqlite3Statement.Exec(
							sqlite3Result,
							query.Input.Parameters...,
						)
					}

					if !query.IsDQL() {
						changes = db.GetConnection().Changes()
						lastInsertRowID = db.GetConnection().SqliteConnection().LastInsertRowID()
					}
				}
			}
		}

		response.SetId(query.Input.Id)
		response.SetLatency(float64(time.Since(start)) / float64(time.Millisecond))

		if transaction != nil || query.IsTransactional() {
			if transaction != nil {
				response.SetTransactionId([]byte(transaction.Id))
			} else {
				response.SetTransactionId([]byte(query.transaction.Id))
			}
		}

		if err != nil {
			response.SetError(err.Error())

			return response, err
		}

		response.SetChanges(changes)
		response.SetLastInsertRowId(lastInsertRowID)

		if sqlite3Result != nil {
			response.SetColumns(sqlite3Result.Columns)
			response.SetRows(sqlite3Result.Rows)
			response.SetRowCount(len(sqlite3Result.Rows))
		}

		logManager.Query(
			logs.QueryLogEntry{
				Cluster:      query.cluster,
				DatabaseHash: query.DatabaseKey.DatabaseHash,
				DatabaseId:   query.DatabaseKey.DatabaseId,
				BranchId:     query.DatabaseKey.BranchId,
				AccessKeyId:  query.AccessKey.AccessKeyId,
				Statement:    query.Input.Statement,
				Latency:      response.Latency(),
			},
		)

		return response, err
	})
}

func resolveWithQueue(
	query *Query,
	response *QueryResponse,
	f func(query *Query, response *QueryResponse) (*QueryResponse, error),
) (*QueryResponse, error) {
	if query.IsWrite() {
		queue := query.databaseManager.WriteQueueManager.GetWriteQueue(query)

		if queue == nil {
			return nil, fmt.Errorf("database not found")
		}

		return queue.Handle(
			func(f func(query *Query, response *QueryResponse) (*QueryResponse, error),
				query *Query,
				response *QueryResponse,
			) (*QueryResponse, error) {
				return f(query, response)
			}, f, query, response)
	}

	return f(query, response)
}

func forwardQueryToPrimary(query *Query, response *QueryResponse) (*QueryResponse, error) {
	responseMessage, err := query.cluster.Node().Send(
		messages.NodeMessage{
			Data: messages.QueryMessage{
				AccessKeyId: query.AccessKey.AccessKeyId,
				BranchId:    query.DatabaseKey.BranchId,
				DatabaseId:  query.DatabaseKey.DatabaseId,
				ID:          query.Input.Id,
				Statement:   query.Input.Statement,
				Parameters:  query.Input.Parameters,
			},
		},
	)

	if err != nil {
		log.Println("Error forwarding query to primary", err)
		response.SetError("error forwarding query to primary")

		return nil, errors.New("error forwarding query to primary")
	}

	switch primaryResponse := responseMessage.Data.(type) {
	case messages.ErrorMessage:
		return nil, fmt.Errorf("%s", primaryResponse.Message)
	case messages.QueryMessageResponse:
		response.SetChanges(primaryResponse.Changes)
		response.SetColumns(primaryResponse.Columns)
		response.SetError(primaryResponse.Error)
		response.SetId(primaryResponse.ID)
		response.SetLatency(primaryResponse.Latency)
		response.SetLastInsertRowId(primaryResponse.LastInsertRowID)
		response.SetRowCount(primaryResponse.RowCount)
		response.SetRows(primaryResponse.Rows)
		response.SetWALSequence(primaryResponse.WALSequence)
		response.SetWALTimestamp(primaryResponse.WALTimestamp)
	default:
		return nil, fmt.Errorf("unexpected response from primary")
	}

	// wal, err := query.databaseManager.Resources(query.DatabaseKey.DatabaseId, query.DatabaseKey.BranchId).WALFile()

	// if err != nil {
	// 	return nil, err
	// }

	// err = response.WaitForReplication(wal)

	// if err != nil {
	// 	return nil, err
	// }

	return response, nil
}

func shouldForwardToPrimary(query *Query) bool {
	return !query.cluster.Node().IsPrimary() &&
		(query.IsPragma() || query.IsDML())
}
