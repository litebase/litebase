package database

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/sqlite3"
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

		if query.IsTransactional() {
			// Handle transactional queries
			db = query.transaction.connection
		} else {
			// Handle non-transactional queries
			db, err = query.databaseManager.ConnectionManager().Get(query.DatabaseKey.DatabaseID, query.DatabaseKey.DatabaseBranchID)

			if err != nil {
				slog.Error("Error getting database connection", "error", err)
				response.SetError(err.Error())

				// Log the error
				logError := logManager.Error(logs.ErrorLogEntry{
					Cluster:      query.cluster,
					DatabaseHash: query.DatabaseKey.DatabaseHash,
					DatabaseID:   query.DatabaseKey.DatabaseID,
					BranchID:     query.DatabaseKey.DatabaseBranchID,
					CredentialID: query.Credential.CredentialID,
					Statement:    query.Input.Statement,
					Error:        err.Error(),
					Latency:      float64(time.Since(start)) / float64(time.Millisecond),
				})

				if logError != nil {
					slog.Error("Error logging error", "error", logError)
				}

				return response, err
			}

			defer query.databaseManager.ConnectionManager().Release(db)

			db = db.WithCredential(query.Credential)
		}

		if query.IsTransactionStart() {
			// Handle transaction begin
			transaction, err = query.databaseManager.Resources(db.Branch).TransactionManager().Create(
				query.cluster,
				query.databaseManager,
				query.DatabaseKey,
				query.Credential,
			)
		} else if query.IsTransactionEnd() {
			// Handle transaction end
			transaction, err = query.databaseManager.Resources(db.Branch).TransactionManager().Get(string(query.Input.TransactionID))

			if err != nil {
				return nil, err
			}

			err = transaction.Commit()
		} else if query.IsTransactionRollback() {
			// Handle transaction rollback
			transaction, err = query.databaseManager.Resources(db.Branch).TransactionManager().Get(string(query.Input.TransactionID))

			if err != nil {
				return nil, err
			}

			err = transaction.Rollback()
		}

		if !query.IsTransactionStart() && !query.IsTransactionEnd() && !query.IsTransactionRollback() {
			if query.IsVacuum() {
				response.SetError(errors.New("VACUUM is not supported from this context").Error())

				// Log the error
				logError := logManager.Error(logs.ErrorLogEntry{
					Cluster:      query.cluster,
					DatabaseHash: query.DatabaseKey.DatabaseHash,
					DatabaseID:   query.DatabaseKey.DatabaseID,
					BranchID:     query.DatabaseKey.DatabaseBranchID,
					CredentialID: query.Credential.CredentialID,
					Statement:    query.Input.Statement,
					Error:        "VACUUM is not supported from this context",
					Latency:      float64(time.Since(start)) / float64(time.Millisecond),
				})

				if logError != nil {
					slog.Error("Error logging error", "error", logError)
				}

				return response, errors.New("VACUUM is not supported from this context")
			} else if query.IsPragma() && IsLitebasePragma(query.Input.Statement) {
				// Handle litebase PRAGMAs directly through Exec
				result, err := db.GetConnection().Exec(query.Input.Statement, query.Input.Parameters)

				if err != nil {
					response.SetError(err.Error())

					// Log the error
					logError := logManager.Error(logs.ErrorLogEntry{
						Cluster:      query.cluster,
						DatabaseHash: query.DatabaseKey.DatabaseHash,
						DatabaseID:   query.DatabaseKey.DatabaseID,
						BranchID:     query.DatabaseKey.DatabaseBranchID,
						CredentialID: query.Credential.CredentialID,
						Statement:    query.Input.Statement,
						Error:        err.Error(),
						Latency:      float64(time.Since(start)) / float64(time.Millisecond),
					})

					if logError != nil {
						slog.Error("Error logging error", "error", logError)
					}

					return response, err
				}

				// Populate response from the PRAGMA result
				if result != nil {
					var firstRow []*sqlite3.Column

					if len(result.Rows) > 0 {
						firstRow = result.Rows[0]
					}

					response.SetColumnsFromResult(result.Columns, nil, firstRow)
					response.SetRows(result.Rows)
					response.SetRowCount(len(result.Rows))
				}
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
						err = statement.Sqlite3Statement.Exec(
							sqlite3Result,
							query.Input.Parameters...,
						)
					}

					if !query.IsDQL() {
						changes = db.GetConnection().Changes()
						lastInsertRowID = db.GetConnection().LastInsertRowID()
					}
				}
			}
		}

		response.SetID(query.Input.ID)
		response.SetLatency(float64(time.Since(start)) / float64(time.Millisecond))

		if transaction != nil || query.IsTransactional() {
			if transaction != nil {
				response.SetTransactionID(transaction.ID)
			} else {
				response.SetTransactionID(query.transaction.ID)
			}
		}

		if err != nil {
			response.SetError(err.Error())

			// Log the error
			logError := logManager.Error(logs.ErrorLogEntry{
				Cluster:      query.cluster,
				DatabaseHash: query.DatabaseKey.DatabaseHash,
				DatabaseID:   query.DatabaseKey.DatabaseID,
				BranchID:     query.DatabaseKey.DatabaseBranchID,
				CredentialID: query.Credential.CredentialID,
				Statement:    query.Input.Statement,
				Error:        err.Error(),
				Latency:      float64(time.Since(start)) / float64(time.Millisecond),
			})

			if logError != nil {
				slog.Error("Error logging error", "error", logError)
			}

			return response, err
		}

		response.SetChanges(changes)
		response.SetLastInsertRowID(lastInsertRowID)

		if sqlite3Result != nil {
			var firstRow []*sqlite3.Column

			if len(sqlite3Result.Rows) > 0 {
				firstRow = sqlite3Result.Rows[0]
			}

			// Use SetColumnsFromResult to avoid allocating a temporary map
			response.SetColumnsFromResult(sqlite3Result.Columns, sqlite3Result.ColumnTypes, firstRow)
			response.SetRows(sqlite3Result.Rows)
			response.SetRowCount(len(sqlite3Result.Rows))
		}

		// Only log queries if query logs are enabled in database branch settings
		var branch *Branch
		if db != nil {
			branch = db.Branch
		} else if transaction != nil {
			branch = transaction.connection.Branch
		}

		if branch.Settings.QueryLogsEnabled {
			err = logManager.Query(
				logs.QueryLogEntry{
					Cluster:      query.cluster,
					DatabaseHash: query.DatabaseKey.DatabaseHash,
					DatabaseID:   query.DatabaseKey.DatabaseID,
					BranchID:     query.DatabaseKey.DatabaseBranchID,
					CredentialID: query.Credential.CredentialID,
					Statement:    query.Input.Statement,
					Latency:      response.Latency(),
				},
			)

			if err != nil {
				slog.Error("Error logging query", "error", err)
			}
		}

		return response, nil
	})
}

func resolveWithQueue(
	query *Query,
	response *QueryResponse,
	f func(query *Query, response *QueryResponse) (*QueryResponse, error),
) (*QueryResponse, error) {
	// Writes that are not part of a transaction should be handled by the queue.
	// Otherwise, queries that are read only should be executed immediately. If
	// a query is transactional, it can be executed immediately as well since
	// the database of the transaction will be holding the necessary locks.
	if (query.IsWrite() && !query.IsTransactional()) || query.IsTransactionStart() {
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
				CredentialID:     query.Credential.CredentialID,
				CredentialScheme: query.Credential.Scheme,
				BranchID:         query.DatabaseKey.DatabaseBranchID,
				DatabaseID:       query.DatabaseKey.DatabaseID,
				ID:               query.Input.ID,
				Statement:        query.Input.Statement,
				Parameters:       query.Input.Parameters,
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
		response.SetID(primaryResponse.ID)
		response.SetLatency(primaryResponse.Latency)
		response.SetLastInsertRowID(primaryResponse.LastInsertRowID)
		response.SetRowCount(primaryResponse.RowCount)
		response.SetRows(primaryResponse.Rows)
		response.SetTransactionID(primaryResponse.TransactionID)
		response.SetWALSequence(primaryResponse.WALSequence)
		response.SetWALTimestamp(primaryResponse.WALTimestamp)
	default:
		return nil, fmt.Errorf("unexpected response from primary")
	}

	// wal, err := query.databaseManager.Resources(query.DatabaseKey.DatabaseID, query.databaseKey.DatabaseBranchID).WALFile()

	// if err != nil {
	// 	return nil, err
	// }

	// err = response.WaitForReplication(wal)

	// if err != nil {
	// 	return nil, err
	// }

	return response, nil
}

// Queries that should be forwarded to the primary node are ones that perform
// write operations or are part of a transaction.
func shouldForwardToPrimary(query *Query) bool {
	return !query.cluster.Node().IsPrimary() &&
		(query.IsPragma() || query.IsDML() || query.IsTransactionStart() || query.IsTransactionEnd() || query.IsTransactionRollback() || query.Input.TransactionID != "")
}
