package database

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash/crc32"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/storage"
	"github.com/litebase/litebase/pkg/vfs"

	"github.com/google/uuid"
)

var (
	ErrDatabaseConnectionClosed = fmt.Errorf("database connection is closed")
)

var DatabaseConnectionConfigStatements = func(config *config.Config) []string {
	return []string{
		fmt.Sprintf("PRAGMA page_size = %d", config.PageSize),

		// Databases should always be in WAL mode. This allows for multiple
		// readers and a single writer.
		"PRAGMA journal_mode=wal",

		// WAL autocheckpoint should be set to 0. This will prevent the WAL
		// file from being checkpointed automatically. Litebase has its own
		// checkpointing mechanism that will be used to checkpoint the WAL.

		// It is very important that this setting remain in place as our the
		// checkpointer is reponsible writing pages to durable storage and
		// properly reporting the page count of the database.
		"PRAGMA wal_autocheckpoint=0",

		// PRAGMA synchronous=NORMAL will ensure that writes to the the database
		// WAL are durable by flushing writes to storage at critical points
		// during database operations.
		"PRAGMA synchronous=NORMAL",

		// PRAGMA busy_timeout will set the timeout for waiting for a lock
		// to 3 seconds. This will allow clients to wait for a lock to be
		// released before returning an error.
		"PRAGMA busy_timeout = 5000",

		// PRAGMA cache_size will set the size of the cache to 0. This will
		// disable caching and force SQLite to read from storage for every query.
		"PRAGMA cache_size = 0",

		// PRAGMA secure_delete will ensure that data is securely deleted from
		// the database. This will prevent data from being recovered from the
		// database file. The added benefit is that it will also reduce the
		// amount of data that needs to be written to durable storage after
		// compression removes data padded with zeros.
		"PRAGMA secure_delete = true",

		// PRAGMA temp_store will set the temp store to memory. This will
		// ensure that temporary files created by SQLite are stored in memory
		// and not on disk.
		"PRAGMA temp_store = memory",

		// PRAGMA foreign_keys will ensure that foreign key constraints are
		// enforced by SQLite.
		"PRAGMA foreign_keys = ON",
	}
}

type DatabaseConnection struct {
	AccessKey              *auth.AccessKey
	branchId               string
	cancel                 context.CancelFunc
	checkpointer           *Checkpointer
	committedAt            time.Time
	config                 *config.Config
	connectionManager      *ConnectionManager
	context                context.Context
	databaseHash           string
	databaseId             string
	fileSystem             *storage.DurableDatabaseFileSystem
	id                     string
	inTransaction          bool
	mutex                  *sync.Mutex
	nodeId                 string
	pageLogger             *storage.PageLogger
	resultPool             *sqlite3.ResultPool
	sqlite3                *sqlite3.Connection
	statements             sync.Map
	transactionalTimestamp int64
	tmpFileSystem          *storage.FileSystem
	vfs                    *vfs.LitebaseVFS
	vfsHash                string
	walManager             *DatabaseWALManager
	walTimestamp           int64
}

// Create a new database connection instance.
func NewDatabaseConnection(connectionManager *ConnectionManager, databaseId, branchId string) (*DatabaseConnection, error) {
	ctx, cancel := context.WithCancel(connectionManager.cluster.Node().Context())

	resources := connectionManager.databaseManager.Resources(databaseId, branchId)

	// Get the database hash for the connection.
	databaseHash := file.DatabaseHash(databaseId, branchId)
	resultPool := resources.ResultPool()
	checkpointer, err := resources.Checkpointer()

	if err != nil {
		cancel()
		slog.Error("Error Getting Checkpointer:", "error", err)

		return nil, err
	}

	walManager, err := resources.DatabaseWALManager()

	if err != nil {
		cancel()
		slog.Error("Error Getting WAL Manager:", "error", err)

		return nil, err
	}

	con := &DatabaseConnection{
		branchId:          branchId,
		cancel:            cancel,
		checkpointer:      checkpointer,
		config:            connectionManager.cluster.Config,
		connectionManager: connectionManager,
		context:           ctx,
		databaseHash:      databaseHash,
		databaseId:        databaseId,
		fileSystem:        connectionManager.databaseManager.Resources(databaseId, branchId).FileSystem(),
		id:                uuid.NewString(),
		mutex:             &sync.Mutex{},
		nodeId:            connectionManager.cluster.Node().ID,
		pageLogger:        connectionManager.databaseManager.Resources(databaseId, branchId).PageLogger(),
		resultPool:        resultPool,
		statements:        sync.Map{},
		tmpFileSystem:     connectionManager.cluster.TmpFS(),
		walManager:        walManager,
		walTimestamp:      time.Now().UTC().UnixNano(),
	}

	err = con.openSqliteConnection()

	if err != nil {
		slog.Error("Error Opening SQLite:", "error", err)

		return nil, err
	}

	return con, err
}

func (con *DatabaseConnection) BusyTimeout(timeout time.Duration) {
	if con.Closed() {
		return
	}

	// Set the busy timeout for the SQLite connection.
	con.sqliteConnection().BusyTimeout(timeout)
}

// Begin a transaction on the database connection
func (con *DatabaseConnection) Begin() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.sqliteConnection().Begin()
}

// Begin a transaction that will deffer the write lock until the first write operation.
func (con *DatabaseConnection) BeginDeferred() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.sqliteConnection().BeginDeferred()
}

// Begin a transaction that will immediately acquire the write lock.
func (con *DatabaseConnection) BeginImmediate() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.sqliteConnection().BeginImmediate()
}

// Commit the current transaction on the database connection
func (con *DatabaseConnection) Commit() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.sqliteConnection().Commit()
}

// Return the number of rows changed by the last statement.
func (con *DatabaseConnection) Changes() int64 {
	if con.Closed() {
		return 0
	}

	return con.sqliteConnection().Changes()
}

// Checkpoint changes that have been made to the database.
func (con *DatabaseConnection) Checkpoint() error {
	if con == nil {
		return nil
	}

	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.checkpointer.CheckpointBarrier(func() error {
		return con.walManager.Checkpoint(func(wal *DatabaseWAL) error {
			// Ensure the timestamp for the checkpoint is acquired on the page logger.
			con.pageLogger.Acquire(wal.timestamp)

			// Ensure the timestamp for the checkpoint is set on the VFS, this will
			// ensure the VFS writes changes from the WAL to the page logger with
			// the correct timestamp. This is crucial for the checkpoint process,
			// as it ensures that the pages are written to the correct location and
			// in the event of a failure, the pages can be tombstoned correctly.
			con.vfs.SetTimestamps(wal.timestamp, time.Now().UTC().UnixNano())

			defer func() {
				con.pageLogger.Release(wal.timestamp)
			}()

			// Begin the checkpoint process using the WAL timestamp.
			err := con.checkpointer.Begin(wal.timestamp)

			if err != nil {
				log.Println("Error beginning checkpoint:", err)
				return err
			}

			_, err = sqlite3.Checkpoint(con.sqliteConnection().Base(), func(result sqlite3.CheckpointResult) error {
				if result.Result != 0 {
					log.Println("Error checkpointing database", err)
				} else {
					err = con.checkpointer.Commit()

					if err != nil {
						slog.Debug("Error checkpointing database", "error", err)
						return err
					} else {
						slog.Debug("Successful database checkpoint")
					}
				}

				return nil
			})

			if err != nil {
				err := con.checkpointer.Rollback()

				if err != nil {
					slog.Error("Error rolling back checkpoint", "error", err)
				}
			} else {
				// Update the WAL Index
				err = con.walManager.Refresh()

				if err != nil {
					slog.Error("Error creating new WAL version:", "error", err)
					return err
				}
			}

			// log.Println("Checkpoint completed successfully")

			return err
		})
	})
}

// Close the database connection.
func (con *DatabaseConnection) Close() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	var err error

	// Finalize all statements before closing the connection.
	err = con.finalizeStatments()

	if err != nil {
		return err
	}

	// Cancel the context of the connection.
	con.cancel()

	// Close the SQLite connection
	err = con.closeSqliteConnection()

	if err != nil {
		slog.Error("Error closing SQLite connection", "error", err)
		return err
	}

	if vfsHash := con.VFSHash(); vfsHash != "" && con.vfs != nil {
		err = vfs.UnregisterVFS(con.VFSHash())

		con.vfs = nil
	}

	return err
}

// Check if the connection is closed.
func (con *DatabaseConnection) Closed() bool {
	return con.sqlite3 == nil
}

// Close the SQLite connection.
func (con *DatabaseConnection) closeSqliteConnection() error {
	if con.sqlite3 != nil {
		if closeErr := con.sqlite3.Close(); closeErr != nil {
			return fmt.Errorf("error closing sqlite3 connection: %w", closeErr)
		}

		con.sqlite3 = nil
	}

	return nil
}

// Return the context of the connection.
func (con *DatabaseConnection) Context() context.Context {
	return con.context
}

func (con *DatabaseConnection) Exec(sql string, parameters []sqlite3.StatementParameter) (result *sqlite3.Result, err error) {
	if con.Closed() {
		return nil, ErrDatabaseConnectionClosed
	}

	result = &sqlite3.Result{}

	var run func(func() error) error

	if !con.inTransaction {
		run = con.walManager.CheckpointBarrier
	} else {
		run = func(fn func() error) error {
			return fn()
		}
	}

	return result, run(func() error {
		// Acquire timestamp inside the checkpoint barrier to ensure atomicity
		con.setTimestamps()
		defer con.releaseTimestamps()

		statement, _, err := con.sqliteConnection().Prepare(con.context, sql)

		if err != nil {
			return err
		}

		err = statement.Exec(result, parameters...)

		if err != nil {
			return err
		}

		con.committedAt = time.Now().UTC()

		return nil
	})
}

func (con *DatabaseConnection) FileSystem() *storage.DurableDatabaseFileSystem {
	return con.fileSystem
}

// Finalize the statements of the connection.
func (con *DatabaseConnection) finalizeStatments() error {
	var err error

	// Ensure all statements are finalized before closing the connection.
	con.statements.Range(func(key any, statement any) bool {
		err = statement.(Statement).Sqlite3Statement.Finalize()

		return true
	})

	if err != nil {
		slog.Error("Error finalizing statement", "error", err)
		return err
	}

	// Clear the statements map
	con.statements = sync.Map{}

	return nil
}

// Return the id of the connection.
func (c *DatabaseConnection) Id() string {
	return c.id
}

// Return the last insert row ID of the connection
func (con *DatabaseConnection) LastInsertRowID() int64 {
	if con.Closed() {
		return 0
	}

	return con.sqliteConnection().LastInsertRowID()
}

func (con *DatabaseConnection) openSqliteConnection() error {
	var err error

	err = con.registerVFS()

	if err != nil {
		slog.Error("Error Registering VFS:", "error", err)

		return err
	}

	path, err := file.GetDatabaseFileTmpPath(
		con.config,
		con.nodeId,
		con.databaseId,
		con.branchId,
	)

	if err != nil {
		log.Println("Error Getting Database File Path:", err)

		return err
	}

	err = file.EnsureDirectoryExists(path)

	if err != nil {
		log.Println("Error Ensuring Directory Exists:", err)

		return err
	}

	con.sqlite3, err = sqlite3.Open(
		con.context,
		path,
		con.VFSHash(),
		sqlite3.SQLITE_OPEN_CREATE|sqlite3.SQLITE_OPEN_READWRITE,
	)

	if err != nil {
		log.Println("Error Opening Database Connection:", err)
		return err
	}

	con.SetAuthorizer()

	// TODO: Verify if this is the proper way to allow replicas to only read.
	if !con.connectionManager.cluster.Node().IsPrimary() {
		// log.Default().Println("Setting database locking mode to EXCLUSIVE")
		// configStatements = append(configStatements, "PRAGMA query_only = true")
	}

	con.setTimestamps()

	for _, statement := range DatabaseConnectionConfigStatements(con.config) {
		_, err = con.sqliteConnection().Exec(con.context, statement)

		if err != nil {
			return err
		}
	}

	con.releaseTimestamps()

	return nil
}

// Prepare a statement for execution.
func (con *DatabaseConnection) Prepare(ctx context.Context, command string) (Statement, error) {
	if con.Closed() {
		return Statement{}, ErrDatabaseConnectionClosed
	}

	statment, _, err := con.sqliteConnection().Prepare(ctx, command)

	if err != nil {
		return Statement{}, err
	}

	return Statement{
		context:          ctx,
		Sqlite3Statement: statment,
	}, nil
}

// Execute a query on the database using a transaction.
func (con *DatabaseConnection) Query(result *sqlite3.Result, statement *sqlite3.Statement, parameters []sqlite3.StatementParameter) error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	err := con.Transaction(statement.IsReadonly(), func(con *DatabaseConnection) error {
		return statement.Exec(result, parameters...)
	})

	if err != nil {
		slog.Error("Error executing query", "error", err)
	}

	return err
}

// Register and instance of the VFS for the database connection.
func (con *DatabaseConnection) registerVFS() error {
	vfs, err := vfs.RegisterVFS(
		con.VFSHash(),
		con.VFSDatabaseHash(),
		con.config.PageSize,
		con.fileSystem,
		con.walManager,
	)

	if err != nil {
		return err
	}

	con.vfs = vfs

	return nil
}

// Release a timestamp from the wal manager and page logger.
func (con *DatabaseConnection) releaseTimestamps() {
	// Release the timestamp from the WAL manager
	con.walManager.Release(con.walTimestamp)

	// Release the timestamp from the page logger
	con.pageLogger.Release(con.walTimestamp)

	// Release the timestamp from the durable database file system
	con.fileSystem.Release(con.transactionalTimestamp)
}

// Return the sqlite3 result pool.
func (con *DatabaseConnection) ResultPool() *sqlite3.ResultPool {
	return con.resultPool
}

// Rollback the current transaction on the database connection
func (con *DatabaseConnection) Rollback() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.sqliteConnection().Rollback()
}

// Set the authorizer for the database connection.
func (c *DatabaseConnection) SetAuthorizer() {
	c.sqliteConnection().Authorizer(func(actionCode int, arg1, arg2, arg3, arg4 string) int32 {
		if c.AccessKey == nil {
			return sqlite3.SQLITE_OK
		}

		allowed := true
		var err error

		switch actionCode {
		case sqlite3.SQLITE_ANALYZE:
			allowed, err = c.AccessKey.CanAnalyze(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_ATTACH:
			allowed, err = c.AccessKey.CanAttach(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_ALTER_TABLE:
			allowed, err = c.AccessKey.CanAlterTable(c.databaseId, c.branchId, arg1, arg2)
		case sqlite3.SQLITE_COPY:
			allowed = false
		case sqlite3.SQLITE_CREATE_INDEX:
			allowed, err = c.AccessKey.CanCreateIndex(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_CREATE_TABLE:
			allowed, err = c.AccessKey.CanCreateTable(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_CREATE_TEMP_TABLE:
			allowed, err = c.AccessKey.CanCreateTempTable(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_CREATE_TEMP_TRIGGER:
			allowed, err = c.AccessKey.CanCreateTempTrigger(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_CREATE_TEMP_VIEW:
			allowed, err = c.AccessKey.CanCreateTempView(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_CREATE_TRIGGER:
			allowed, err = c.AccessKey.CanCreateTrigger(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_CREATE_VIEW:
			allowed, err = c.AccessKey.CanCreateView(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_CREATE_VTABLE:
			allowed, err = c.AccessKey.CanCreateVTable(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_DELETE:
			allowed, err = c.AccessKey.CanDelete(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_DETACH:
			allowed, err = c.AccessKey.CanDetach(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_DROP_INDEX:
			allowed, err = c.AccessKey.CanDropIndex(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_DROP_TABLE:
			allowed, err = c.AccessKey.CanDropTable(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_DROP_TRIGGER:
			allowed, err = c.AccessKey.CanDropTrigger(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_DROP_VIEW:
			allowed, err = c.AccessKey.CanDropView(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_FUNCTION:
			allowed, err = c.AccessKey.CanFunction(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_INSERT:
			allowed, err = c.AccessKey.CanInsert(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_PRAGMA:
			allowed, err = c.AccessKey.CanPragma(c.databaseId, c.branchId, arg1, arg2)
		case sqlite3.SQLITE_READ:
			allowed, err = c.AccessKey.CanRead(c.databaseId, c.branchId, arg1, arg2)
		case sqlite3.SQLITE_RECURSIVE:
			allowed, err = c.AccessKey.CanRecursive(c.databaseId, c.branchId)
		case sqlite3.SQLITE_REINDEX:
			allowed, err = c.AccessKey.CanReindex(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_SAVEPOINT:
			allowed, err = c.AccessKey.CanSavepoint(c.databaseId, c.branchId, arg1, arg2)
		case sqlite3.SQLITE_SELECT:
			allowed, err = c.AccessKey.CanSelect(c.databaseId, c.branchId)
		case sqlite3.SQLITE_TRANSACTION:
			allowed, err = c.AccessKey.CanTransaction(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_UPDATE:
			allowed, err = c.AccessKey.CanUpdate(c.databaseId, c.branchId, arg1, arg2)
		default:
			allowed, err = false, nil
		}

		if err != nil {
			c.sqliteConnection().SetAuthorizationError(err)

			return sqlite3.SQLITE_DENY
		}

		if allowed {
			return sqlite3.SQLITE_OK
		}

		return sqlite3.SQLITE_DENY
	})
}

func (con *DatabaseConnection) setTimestamps() {
	// First acquire WAL timestamp without holding the connection lock
	// to avoid potential deadlocks with WAL manager
	timestamp, err := con.walManager.Acquire()

	if err != nil {
		slog.Error("Error acquiring WAL timestamp:", "error", err)
		return
	}

	con.walTimestamp = timestamp

	// Also, define a transactional timestamp for the start of the transaction
	con.transactionalTimestamp = time.Now().UTC().UnixNano()

	// Acquire the timestamp on the page logger
	con.pageLogger.Acquire(con.walTimestamp)

	// Acquire the timestamp on the durable database file system
	con.fileSystem.Acquire(con.transactionalTimestamp)

	// Set timestamp on VFS for proper WAL file reading
	con.vfs.SetTimestamps(con.walTimestamp, con.transactionalTimestamp)
}

// Return the underlying sqlite3 connection of the database connection.
func (con *DatabaseConnection) sqliteConnection() *sqlite3.Connection {
	return con.sqlite3
}

// Create a statement for a query.
func (con *DatabaseConnection) Statement(queryStatement string) (Statement, error) {
	if con.Closed() {
		return Statement{}, ErrDatabaseConnectionClosed
	}

	var err error

	checksum := crc32.ChecksumIEEE([]byte(queryStatement))

	statement, ok := con.statements.Load(checksum)

	if !ok {
		statement, err = con.Prepare(con.context, queryStatement)

		if err == nil {
			con.statements.Store(checksum, statement)
		}
	}

	return statement.(Statement), err
}

// Execute a transaction on the database.
func (con *DatabaseConnection) Transaction(
	readOnly bool,
	handler func(con *DatabaseConnection) error,
) error {
	con.inTransaction = true

	defer func() {
		con.inTransaction = false
	}()

	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.walManager.CheckpointBarrier(func() error {
		var err error

		// Acquire timestamp inside the checkpoint barrier to ensure atomicity
		con.setTimestamps()

		defer func() {
			con.releaseTimestamps()
		}()

		if !readOnly {
			// Start the transaction with a write lock.
			err = con.sqliteConnection().BeginImmediate()
		} else {
			err = con.sqliteConnection().BeginDeferred()
		}

		if err != nil {
			return err
		}

		handlerError := handler(con)

		if handlerError != nil {
			err = con.sqliteConnection().Rollback()

			if err != nil {
				log.Println("Transaction Error:", err)
			}

			return handlerError
		}

		err = con.sqliteConnection().Commit()

		if err != nil {
			log.Println("Transaction Error:", err)
			return err
		}

		if !readOnly {
			con.committedAt = time.Now().UTC()
		}

		return handlerError
	})
}

func (con *DatabaseConnection) Vacuum() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	return con.sqliteConnection().Vacuum()
}

func (con *DatabaseConnection) VFSDatabaseHash() string {
	return fmt.Sprintf("%s:%s", con.nodeId, con.databaseHash)
}

// Return the VFS hash for the connection.
func (con *DatabaseConnection) VFSHash() string {
	if con.vfsHash == "" {
		sha256Hash := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%s", con.databaseId, con.branchId, con.id))
		con.vfsHash = fmt.Sprintf("litebase:%x", sha256Hash)
	}

	return con.vfsHash
}

func (con *DatabaseConnection) WALTimestamp() int64 {
	return con.walTimestamp
}

// Set the access key for the database connection.
func (con *DatabaseConnection) WithAccessKey(accessKey *auth.AccessKey) *DatabaseConnection {
	con.AccessKey = accessKey

	return con
}
