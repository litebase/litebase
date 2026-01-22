package database

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"log/slog"
	"sync"
	"time"
	"unsafe"

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
	noopBarrier                 = func(fn func() error) error { return fn() }
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
		// checkpointer is responsible writing pages to durable storage and
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
	branch                 *Branch
	cancel                 context.CancelFunc
	checkpointer           *Checkpointer
	committedAt            time.Time
	config                 *config.Config
	connectionManager      *ConnectionManager
	context                context.Context
	databaseHash           string
	Credential             *auth.Credential
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
	// Temporary fields for exec without closure allocation
	// execSQL        string
	// execResult     *sqlite3.Result
	// execParameters []sqlite3.StatementParameter
	// Temporary fields for transaction without closure allocation
	transactionReadOnly bool
	// transactionHandler  func(con *DatabaseConnection) error
	// Temporary fields for query without closure allocation
	// queryResult     *sqlite3.Result
	// queryStatement  *sqlite3.Statement
	// queryParameters []sqlite3.StatementParameter
}

type StatementKey struct {
	SQLChecksum        uint32
	CredentialCheckSum [32]byte
}

// Create a new database connection instance.
func NewDatabaseConnection(connectionManager *ConnectionManager, branch *Branch) (*DatabaseConnection, error) {
	ctx, cancel := context.WithCancel(connectionManager.cluster.Node().Context())

	resources := connectionManager.databaseManager.Resources(branch)

	// Get the database hash for the connection.
	databaseHash := file.DatabaseHash(branch.DatabaseID, branch.DatabaseBranchID)
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
		branch:            branch,
		cancel:            cancel,
		checkpointer:      checkpointer,
		config:            connectionManager.cluster.Config,
		connectionManager: connectionManager,
		context:           ctx,
		databaseHash:      databaseHash,
		fileSystem:        resources.FileSystem(),
		id:                uuid.NewString(),
		mutex:             &sync.Mutex{},
		nodeId:            connectionManager.cluster.Node().ID,
		pageLogger:        resources.PageLogger(),
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

func (con *DatabaseConnection) BusyTimeout(timeout time.Duration) error {
	if con.Closed() {
		return nil
	}

	// Set the busy timeout for the SQLite connection.
	return con.sqliteConnection().BusyTimeout(timeout)
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

	defer func() {
		con.vfs.WALUpdated()
	}()

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

	return con.checkpointer.CheckpointBarrier(con.executeCheckpoint)
}

// executeCheckpoint performs the checkpoint without closure allocation
func (con *DatabaseConnection) executeCheckpoint() error {
	return con.walManager.Checkpoint(con.performCheckpointOnWAL)
}

// performCheckpointOnWAL executes checkpoint operations on a specific WAL without closure allocation
func (con *DatabaseConnection) performCheckpointOnWAL(wal *DatabaseWAL) error {
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

	return err
}

// Close the database connection.
func (con *DatabaseConnection) Close() error {
	if con.Closed() {
		return ErrDatabaseConnectionClosed
	}

	var err error

	// Finalize all statements before closing the connection.
	err = con.finalizestatements()

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

	// Check if this is a litebase PRAGMA statement
	if IsLitebasePragma(sql) {
		return con.execLitebasePragma(sql)
	}

	result = con.resultPool.Get()

	var checkpointBarrier func(func() error) error
	var compactionBarrier func(func() error) error

	if !con.inTransaction {
		checkpointBarrier = con.walManager.CheckpointBarrier
		compactionBarrier = con.fileSystem.CompactionBarrier
	} else {
		checkpointBarrier = func(fn func() error) error {
			return fn()
		}

		compactionBarrier = func(fn func() error) error {
			return fn()
		}
	}

	return result, checkpointBarrier(func() error {
		return compactionBarrier(func() error {
			con.mutex.Lock()
			defer con.mutex.Unlock()

			// Acquire timestamp inside the checkpoint barrier to ensure atomicity
			con.setTimestamps()
			defer con.releaseTimestamps()

			statement, err := con.Statement(sql)

			if err != nil {
				return err
			}

			err = statement.Sqlite3Statement.Exec(result, parameters...)

			if err != nil {
				return err
			}

			if con.sqliteConnection().Changes() > 0 {
				con.committedAt = time.Now().UTC()

				con.vfs.WALUpdated()
			}

			return nil
		})
	})
}

// execLitebasePragma handles execution of custom litebase PRAGMA statements
func (con *DatabaseConnection) execLitebasePragma(sql string) (*sqlite3.Result, error) {
	handler := NewLitebasePragmaHandler(con, con.branch.DatabaseID, con.branch.DatabaseBranchID)

	value, err := handler.Execute(sql)

	if err != nil {
		return nil, err
	}

	result := &sqlite3.Result{}

	// If it's a GET operation, return the value as a result
	if value != nil {
		result.Columns = []string{"value"}

		var column *sqlite3.Column
		switch v := value.(type) {
		case int:
			// Convert int to bytes (8-byte little-endian)
			valueBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(valueBytes, uint64(v))
			column = sqlite3.NewColumn(sqlite3.ColumnTypeInteger, valueBytes)
		case string:
			column = sqlite3.NewColumn(sqlite3.ColumnTypeText, []byte(v))
		default:
			return nil, fmt.Errorf("unsupported PRAGMA value type: %T", value)
		}

		result.Rows = [][]*sqlite3.Column{
			{column},
		}
	}

	return result, nil
}

func (con *DatabaseConnection) FileSystem() *storage.DurableDatabaseFileSystem {
	return con.fileSystem
}

// Finalize the statements of the connection.
func (con *DatabaseConnection) finalizestatements() error {
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
		con.branch.DatabaseID,
		con.branch.DatabaseBranchID,
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

	// Open SQLite connection within the barriers to prevent corruption
	err = con.checkpointer.CheckpointPassiveBarrier(func() error {
		con.sqlite3, err = sqlite3.Open(
			con.context,
			path,
			con.VFSHash(),
			sqlite3.SQLITE_OPEN_CREATE|sqlite3.SQLITE_OPEN_READWRITE,
		)

		if err != nil {
			return err
		}

		// Set authorizer immediately after opening
		con.SetAuthorizer()

		// Set the authorizer for the connection
		con.setTimestamps()

		return nil
	})

	if err != nil {
		log.Println("Error Opening Database Connection:", err)
		return err
	}

	// TODO: Verify if this is will enforce replicas to only perform reads.
	// if !con.connectionManager.cluster.Node().IsPrimary() {
	// configStatements = append(configStatements, "PRAGMA query_only = true")
	// }

	// Execute configuration statements with timestamps set
	for _, statement := range DatabaseConnectionConfigStatements(con.config) {
		_, err = con.sqliteConnection().Exec(con.context, statement)

		if err != nil {
			con.releaseTimestamps()
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

	statement, _, err := con.sqliteConnection().Prepare(ctx, command)

	if err != nil {
		return Statement{}, err
	}

	return Statement{
		context:          ctx,
		Sqlite3Statement: statement,
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
		con.databaseHash,
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
	// Release timestamps in reverse order of acquisition to avoid deadlocks

	// Release the timestamp from the page logger
	if con.pageLogger != nil {
		con.pageLogger.Release(con.walTimestamp)
	}

	// Release the timestamp from the WAL manager
	if con.walManager != nil {
		con.walManager.Release(con.walTimestamp)
	}
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
		if c.Credential == nil {
			return sqlite3.SQLITE_OK
		}

		var allowed bool
		var err error

		switch actionCode {
		case sqlite3.SQLITE_ANALYZE:
			allowed, err = c.Credential.CanAnalyze(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_ATTACH:
			allowed, err = c.Credential.CanAttach(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_ALTER_TABLE:
			allowed, err = c.Credential.CanAlterTable(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1, arg2)
		case sqlite3.SQLITE_COPY:
			allowed = false
		case sqlite3.SQLITE_CREATE_INDEX:
			allowed, err = c.Credential.CanCreateIndex(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg2, arg1)
		case sqlite3.SQLITE_CREATE_TABLE:
			allowed, err = c.Credential.CanCreateTable(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_CREATE_TEMP_TABLE:
			allowed, err = c.Credential.CanCreateTempTable(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_CREATE_TEMP_TRIGGER:
			allowed, err = c.Credential.CanCreateTempTrigger(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg2, arg1)
		case sqlite3.SQLITE_CREATE_TEMP_VIEW:
			allowed, err = c.Credential.CanCreateTempView(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_CREATE_TRIGGER:
			allowed, err = c.Credential.CanCreateTrigger(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg2, arg1)
		case sqlite3.SQLITE_CREATE_VIEW:
			allowed, err = c.Credential.CanCreateView(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_CREATE_VTABLE:
			allowed, err = c.Credential.CanCreateVTable(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg2, arg1)
		case sqlite3.SQLITE_DELETE:
			allowed, err = c.Credential.CanDelete(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_DETACH:
			allowed, err = c.Credential.CanDetach(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_DROP_INDEX:
			allowed, err = c.Credential.CanDropIndex(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg2, arg1)
		case sqlite3.SQLITE_DROP_TABLE:
			allowed, err = c.Credential.CanDropTable(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_DROP_TRIGGER:
			allowed, err = c.Credential.CanDropTrigger(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg2, arg1)
		case sqlite3.SQLITE_DROP_VIEW:
			allowed, err = c.Credential.CanDropView(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_FUNCTION:
			allowed, err = c.Credential.CanFunction(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_INSERT:
			allowed, err = c.Credential.CanInsert(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_PRAGMA:
			allowed, err = c.Credential.CanPragma(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1, arg2)
		case sqlite3.SQLITE_READ:
			allowed, err = c.Credential.CanRead(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1, arg2)
		case sqlite3.SQLITE_RECURSIVE:
			allowed, err = c.Credential.CanRecursive(c.branch.DatabaseID, c.branch.DatabaseBranchID)
		case sqlite3.SQLITE_REINDEX:
			allowed, err = c.Credential.CanReindex(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_SAVEPOINT:
			allowed, err = c.Credential.CanSavepoint(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1, arg2)
		case sqlite3.SQLITE_SELECT:
			allowed, err = c.Credential.CanSelect(c.branch.DatabaseID, c.branch.DatabaseBranchID)
		case sqlite3.SQLITE_TRANSACTION:
			allowed, err = c.Credential.CanTransaction(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1)
		case sqlite3.SQLITE_UPDATE:
			allowed, err = c.Credential.CanUpdate(c.branch.DatabaseID, c.branch.DatabaseBranchID, arg1, arg2)
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

	// Set timestamp on VFS for proper WAL file reading
	// Only set if VFS is available to avoid nil pointer dereference
	if con.vfs != nil {
		con.vfs.SetTimestamps(con.walTimestamp, con.transactionalTimestamp)
	}
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

	sqlChecksum := crc32.ChecksumIEEE(unsafe.Slice(unsafe.StringData(queryStatement), len(queryStatement)))

	credentialChecksum := [32]byte{}

	if con.Credential != nil {
		credentialChecksum = con.Credential.Hash()
	}

	statementKey := StatementKey{
		SQLChecksum:        sqlChecksum,
		CredentialCheckSum: credentialChecksum,
	}

	statement, ok := con.statements.Load(statementKey)

	if !ok {
		statement, err = con.Prepare(con.context, queryStatement)

		if err == nil {
			con.statements.Store(statementKey, statement)
		}
	}

	return statement.(Statement), err
}

// Execute a transaction on the database.
func (con *DatabaseConnection) Transaction(
	readOnly bool,
	handler func(con *DatabaseConnection) error,
) error {
	con.mutex.Lock()
	con.inTransaction = true
	con.mutex.Unlock()

	defer func() {
		con.mutex.Lock()
		con.inTransaction = false
		con.mutex.Unlock()
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
			con.mutex.Lock()
			con.committedAt = time.Now().UTC()
			con.mutex.Unlock()
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
		sha256Hash := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%s", con.branch.DatabaseID, con.branch.DatabaseBranchID, con.id))
		con.vfsHash = fmt.Sprintf("litebase:%x", sha256Hash)
	}

	return con.vfsHash
}

func (con *DatabaseConnection) WALTimestamp() int64 {
	return con.walTimestamp
}

// Set the access key for the database connection.
func (con *DatabaseConnection) WithCredential(credential *auth.Credential) *DatabaseConnection {
	con.Credential = credential

	return con
}
