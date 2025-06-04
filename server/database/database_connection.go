package database

import (
	"context"
	"crypto/sha1"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
	"time"
	"unsafe"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/constants"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/sqlite3"
	"github.com/litebase/litebase/server/storage"
	"github.com/litebase/litebase/server/vfs"

	"github.com/google/uuid"
)

type DatabaseConnection struct {
	AccessKey         *auth.AccessKey
	branchId          string
	cancel            context.CancelFunc
	checkpointer      *Checkpointer
	committedAt       time.Time
	config            *config.Config
	connectionManager *ConnectionManager
	context           context.Context
	databaseHash      string
	databaseId        string
	id                string
	fileSystem        *storage.DurableDatabaseFileSystem
	mutex             *sync.Mutex
	nodeId            uint64
	pageLogger        *storage.PageLogger
	resultPool        *sqlite3.ResultPool
	sqlite3           *sqlite3.Connection
	statements        sync.Map
	timestamp         int64
	tmpFileSystem     *storage.FileSystem
	vfs               *vfs.LitebaseVFS
	vfsHash           string
	walManager        *DatabaseWALManager
}

// Create a new database connection instance.
func NewDatabaseConnection(connectionManager *ConnectionManager, databaseId, branchId string) (*DatabaseConnection, error) {
	var (
		connection *sqlite3.Connection
		err        error
	)

	ctx, cancel := context.WithCancel(context.Background())
	resources := connectionManager.databaseManager.Resources(databaseId, branchId)
	// Get the database hash for the connection.
	databaseHash := file.DatabaseHash(databaseId, branchId)
	resultPool := resources.ResultPool()
	checkpointer, err := resources.Checkpointer()

	if err != nil {
		cancel()
		log.Println("Error Getting Checkpointer:", err)

		return nil, err
	}

	walManager, err := resources.DatabaseWALManager()

	if err != nil {
		log.Println("Error Getting WAL Manager:", err)
		cancel()

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
		timestamp:         time.Now().UnixMicro(),
		tmpFileSystem:     connectionManager.cluster.TmpFS(),
		walManager:        walManager,
	}

	err = con.registerVFS()

	if err != nil {
		log.Println("Error Registering VFS:", err)

		return nil, err
	}

	path, err := file.GetDatabaseFileTmpPath(
		con.config,
		con.nodeId,
		databaseId,
		branchId,
	)

	if err != nil {
		log.Println("Error Getting Database File Path:", err)

		return nil, err
	}

	err = file.EnsureDirectoryExists(path)

	if err != nil {
		log.Println("Error Ensuring Directory Exists:", err)

		return nil, err
	}

	connection, err = sqlite3.Open(
		con.context,
		path,
		con.VFSHash(),
		sqlite3.SQLITE_OPEN_CREATE|sqlite3.SQLITE_OPEN_READWRITE,
	)

	if err != nil {
		log.Println("Error Opening Database Connection:", err)
		return nil, err
	}

	con.sqlite3 = connection

	con.SetAuthorizer()

	configStatements := [][]byte{
		[]byte(fmt.Sprintf("PRAGMA page_size = %d", con.config.PageSize)),

		// Databases should always be in WAL mode. This allows for multiple
		// readers and a single writer.
		[]byte("PRAGMA journal_mode=wal"),

		// WAL autocheckpoint should be set to 0. This will prevent the WAL
		// file from being checkpointed automatically. Litebase has its own
		// checkpointing mechanism that will be used to checkpoint the WAL.

		// It is very important that this setting remain in place as our the
		// checkpointer is reponsible writing pages to durable storage and
		// properly reporting the page count of the database.
		[]byte("PRAGMA wal_autocheckpoint=0"),

		// PRAGMA synchronous=NORMAL will ensure that the database is durable
		// by writing to the WAL file before the transaction is committed.
		[]byte("PRAGMA synchronous=NORMAL"),

		// PRAGMA busy_timeout will set the timeout for waiting for a lock
		// to 3 seconds. This will allow clients to wait for a lock to be
		// released before returning an error.
		[]byte("PRAGMA busy_timeout = 5000"),

		// The amount of cache that SQLite will use is set to -2000000. This
		// will allow SQLite to use as much memory as it needs for caching.
		[]byte("PRAGMA cache_size = 0"),
		// []byte("PRAGMA cache_size = -2000"),
		// []byte("PRAGMA cache_size = 20000000"),

		// PRAGMA secure_delete will ensure that data is securely deleted from
		// the database. This will prevent data from being recovered from the
		// database file. The added benefit is that it will also reduce the
		// amount of data that needs to be written to durable storage after
		// compression removes data padded with zeros.
		[]byte("PRAGMA secure_delete = true"),

		// PRAGMA temp_store will set the temp store to memory. This will
		// ensure that temporary files created by SQLite are stored in memory
		// and not on disk.
		// []byte("PRAGMA temp_store = memory"),
	}

	if !con.connectionManager.cluster.Node().IsPrimary() {
		// log.Default().Println("Setting database locking mode to EXCLUSIVE")
		// configStatements = append(configStatements, "PRAGMA query_only = true")
	}

	con.setTimestamp()

	for _, statement := range configStatements {
		_, err = con.sqlite3.Exec(ctx, statement)

		if err != nil {
			return nil, err
		}
	}

	con.releaseTimestamp()

	return con, err
}

// Return the number of rows changed by the last statement.
func (con *DatabaseConnection) Changes() int64 {
	return con.sqlite3.Changes()
}

// Checkpoint changes that have been made to the database.
func (con *DatabaseConnection) Checkpoint() error {
	start := time.Now()
	defer func() {
		log.Println("Checkpoint took", time.Since(start))
	}()

	if con == nil || con.sqlite3 == nil {
		return nil
	}

	// Get the latest WAL for the database.
	wal, err := con.walManager.Get(time.Now().UnixMicro())

	if err != nil {
		log.Println("Error acquiring WAL :", err)
		return err
	}

	return con.walManager.Checkpoint(wal, func() error {
		// Ensure the timestamp for the checkpoint is acquired on the page logger.
		con.pageLogger.Acquire(wal.timestamp)

		// Ensure the timestamp for the checkpoint is set on the VFS, this will
		// ensure the VFS writes changes from the WAL to the page logger with
		// the correct timestamp. This is crucial for the checkpoint process,
		// as it ensures that the pages are written to the correct location and
		// in the event of a failure, the pages can be tombstoned correctly.
		con.vfs.SetTimestamp(wal.timestamp)

		defer func() {
			con.pageLogger.Release(wal.timestamp)
		}()

		// Begin the checkpoint process using the WAL timestamp.
		err = con.checkpointer.Begin(wal.timestamp)

		if err != nil {
			log.Println("Error beginning checkpoint:", err)
			return err
		}

		_, err = sqlite3.Checkpoint(con.sqlite3.Base(), func(result sqlite3.CheckpointResult) error {
			if result.Result != 0 {
				log.Println("Error checkpointing database", err)
			} else {
				err = con.checkpointer.Commit()

				if err != nil {
					log.Println("Error checkpointing database", err)
					return err
				} else {
					// log.Println("Successful database checkpoint")
				}
			}

			return nil
		})

		if err != nil {
			con.checkpointer.Rollback()
		} else {
			// Update the WAL Index
			err = con.walManager.Refresh()

			if err != nil {
				log.Println("Error creating new WAL version:", err)
				return err
			}
		}

		return err
	})
}

// Close the database connection.
func (con *DatabaseConnection) Close() error {
	var err error

	// Ensure all statements are finalized before closing the connection.
	con.statements.Range(func(key any, statement any) bool {
		err = statement.(Statement).Sqlite3Statement.Finalize()

		return true
	})

	if err != nil {
		log.Println("Error finalizing statement:", err)

		return err
	}

	// Cancel the context of the connection.
	con.cancel()

	con.statements = sync.Map{}

	if con.sqlite3 != nil {
		err = con.sqlite3.Close()

		if err != nil {
			return err
		}
	}

	con.release()

	err = vfs.UnregisterVFS(con.VFSHash())

	con.sqlite3 = nil

	return err
}

// Check if the connection is closed.
func (con *DatabaseConnection) Closed() bool {
	return con.sqlite3 == nil
}

// Return the context of the connection.
func (con *DatabaseConnection) Context() context.Context {
	return con.context
}

func (con *DatabaseConnection) Exec(sql string, parameters []sqlite3.StatementParameter) (result *sqlite3.Result, err error) {
	result = &sqlite3.Result{}
	statement, _, err := con.SqliteConnection().Prepare(con.context, []byte(sql))

	if err != nil {
		return nil, err
	}

	err = statement.Exec(result, parameters...)

	return result, err
}

func (con *DatabaseConnection) FileSystem() *storage.DurableDatabaseFileSystem {
	return con.fileSystem
}

// Return the id of the connection.
func (c *DatabaseConnection) Id() string {
	return c.id
}

// Prepare a statement for execution.
func (con *DatabaseConnection) Prepare(ctx context.Context, command []byte) (Statement, error) {
	statment, _, err := con.sqlite3.Prepare(ctx, command)

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
	err := con.Transaction(statement.IsReadonly(), func(con *DatabaseConnection) error {
		return statement.Exec(result, parameters...)
	})

	if err != nil {
		log.Println("Error executing query:", err, err == constants.ServerErrors[constants.ErrSnapshotConflict])
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

// Release the connection.
func (con *DatabaseConnection) release() {
	if con.timestamp > 0 {
		// con.walManager.Release(con.timestamp)
	}
}

func (con *DatabaseConnection) releaseTimestamp() {
	con.walManager.Release(con.timestamp)
	con.pageLogger.Release(con.timestamp)
}

func (con *DatabaseConnection) ResultPool() *sqlite3.ResultPool {
	return con.resultPool
}

// Set the authorizer for the database connection.
func (c *DatabaseConnection) SetAuthorizer() {
	c.sqlite3.Authorizer(func(actionCode int, arg1, arg2, arg3, arg4 string) int {
		// log.Println("Authorizer Action Code:", actionCode)
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
		case sqlite3.SQLITE_CREATE_TEMP_INDEX:
			allowed, err = c.AccessKey.CanCreateTempIndex(c.databaseId, c.branchId, arg2, arg1)
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
		case sqlite3.SQLITE_DROP_TEMP_INDEX:
			allowed, err = c.AccessKey.CanDropTempIndex(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_DROP_TEMP_TABLE:
			allowed, err = c.AccessKey.CanDropTempTable(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_DROP_TEMP_TRIGGER:
			allowed, err = c.AccessKey.CanDropTempTrigger(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_DROP_TEMP_VIEW:
			allowed, err = c.AccessKey.CanDropTempView(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_DROP_TRIGGER:
			allowed, err = c.AccessKey.CanDropTrigger(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_DROP_VIEW:
			allowed, err = c.AccessKey.CanDropView(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_DROP_VTABLE:
			allowed, err = c.AccessKey.CanDropVTable(c.databaseId, c.branchId, arg2, arg1)
		case sqlite3.SQLITE_FUNCTION:
			allowed, err = c.AccessKey.CanFunction(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_INSERT:
			allowed, err = c.AccessKey.CanInsert(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_PRAGMA:
			allowed, err = c.AccessKey.CanPragma(c.databaseId, c.branchId, arg1, arg2)
		case sqlite3.SQLITE_READ:
			allowed, err = c.AccessKey.CanRead(c.databaseId, c.branchId, arg3, arg4)
		case sqlite3.SQLITE_RECURSIVE:
			allowed, err = c.AccessKey.CanRecursive(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_REINDEX:
			allowed, err = c.AccessKey.CanReindex(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_SAVEPOINT:
			allowed, err = c.AccessKey.CanSavepoint(c.databaseId, c.branchId, arg1, arg2)
		case sqlite3.SQLITE_SELECT:
			allowed, err = c.AccessKey.CanSelect(c.databaseId, c.branchId)
		case sqlite3.SQLITE_TRANSACTION:
			allowed, err = c.AccessKey.CanTransaction(c.databaseId, c.branchId, arg1)
		case sqlite3.SQLITE_UPDATE:
			allowed, err = c.AccessKey.CanUpdate(c.databaseId, c.branchId, arg3, arg4)
		default:
			allowed, err = false, nil
		}

		if err != nil {
			c.SqliteConnection().SetAuthorizationError(err)

			return sqlite3.SQLITE_DENY
		}

		if actionCode == sqlite3.SQLITE_SELECT && !allowed {
			return sqlite3.SQLITE_IGNORE
		}

		if allowed {
			return sqlite3.SQLITE_OK
		}

		return sqlite3.SQLITE_DENY
	})
}

func (con *DatabaseConnection) setTimestamp() {
	wal, err := con.walManager.Get(time.Now().UnixMicro())

	if err != nil {
		log.Println("Error acquiring WAL timestamp:", err)
		return
	}

	con.timestamp = wal.timestamp
	con.pageLogger.Acquire(con.timestamp)
	con.vfs.SetTimestamp(con.timestamp)
}

// Return the underlying sqlite3 connection of the database connection.
func (con *DatabaseConnection) SqliteConnection() *sqlite3.Connection {
	return con.sqlite3
}

// Create a statement for a query.
func (con *DatabaseConnection) Statement(queryStatement []byte) (Statement, error) {
	var err error

	checksum := crc32.ChecksumIEEE(unsafe.Slice(unsafe.SliceData(queryStatement), len(queryStatement)))

	statement, ok := con.statements.Load(checksum)

	if !ok {
		statement, err = con.Prepare(con.context, queryStatement)

		if err == nil {
			con.statements.Store(checksum, statement)
		}
	}

	return statement.(Statement), err
}

func (con *DatabaseConnection) Timestamp() int64 {
	return con.timestamp
}

// Execute a transaction on the database.
func (con *DatabaseConnection) Transaction(
	readOnly bool,
	handler func(con *DatabaseConnection) error,
) error {
	var err error

	return con.walManager.CheckpointBarrier(func() error {
		// Set connection timestamp before starting the transaction. This ensures we
		// have a consistent timestamp for the transaction and the vfs reads from
		// the proper WAL file and Page Log.
		con.setTimestamp()

		defer func() {
			con.releaseTimestamp()
		}()

		if !readOnly {
			// Start the transaction with a write lock.
			err = con.SqliteConnection().BeginImmediate()
		} else {
			err = con.SqliteConnection().BeginDeferred()
		}

		if err != nil {
			return err
		}

		handlerError := handler(con)

		if handlerError != nil {
			err = con.SqliteConnection().Rollback()

			if err != nil {
				log.Println("Transaction Error:", err)
			}

			return handlerError
		}

		err = con.SqliteConnection().Commit()

		if err != nil {
			log.Println("Transaction Error:", err)
			return err
		}

		if !readOnly {
			con.committedAt = time.Now()
		}

		return handlerError
	})
}

func (con *DatabaseConnection) VFSDatabaseHash() string {
	return fmt.Sprintf("%s:%s", con.nodeId, con.databaseHash)
}

// Return the VFS hash for the connection.
func (con *DatabaseConnection) VFSHash() string {
	if con.vfsHash == "" {
		sha1 := sha1.New()
		sha1.Write(fmt.Appendf(nil, "%s:%s:%s", con.databaseId, con.branchId, con.id))
		con.vfsHash = fmt.Sprintf("litebase:%x", sha1.Sum(nil))
	}

	return con.vfsHash

}

// Set the access key for the database connection.
func (con *DatabaseConnection) WithAccessKey(accessKey *auth.AccessKey) *DatabaseConnection {
	con.AccessKey = accessKey

	return con
}
