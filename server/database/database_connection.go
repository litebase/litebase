package database

import (
	"context"
	"crypto/sha1"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
	"time"

	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/file"
	"litebase/server/node"
	"litebase/server/sqlite3"
	"litebase/server/storage"
	"litebase/server/vfs"

	"github.com/google/uuid"
)

type DatabaseConnection struct {
	accessKey      auth.AccessKey
	branchUuid     string
	cancel         context.CancelFunc
	checkpointer   *Checkpointer
	committedAt    time.Time
	committing     bool
	context        context.Context
	databaseHash   string
	databaseUuid   string
	id             string
	fileSystem     storage.DatabaseFileSystem
	sqlite3        *sqlite3.Connection
	statements     sync.Map
	tempFileSystem storage.DatabaseFileSystem
	vfsHash        string
	walTimestamp   int64
}

func NewDatabaseConnection(databaseUuid, branchUuid string, walTimestamp int64) (*DatabaseConnection, error) {
	var (
		connection *sqlite3.Connection
		err        error
	)

	ctx, canel := context.WithCancel(context.Background())

	var databaseHash string
	var tempFileSystem storage.DatabaseFileSystem

	// if node.Node().IsPrimary() {
	databaseHash = file.DatabaseHash(databaseUuid, branchUuid)
	tempFileSystem = DatabaseResources().TempFileSystem(databaseUuid, branchUuid)
	// } else {
	// 	databaseHash = file.DatabaseHashWithTimestamp(databaseUuid, branchUuid, walTimestamp)
	// 	tempFileSystem = DatabaseResources().TempFileSystemWithTimestamp(databaseUuid, branchUuid, walTimestamp)
	// }

	con := &DatabaseConnection{
		branchUuid:     branchUuid,
		cancel:         canel,
		checkpointer:   DatabaseResources().Checkpointer(databaseUuid, branchUuid),
		context:        ctx,
		databaseHash:   databaseHash,
		databaseUuid:   databaseUuid,
		fileSystem:     DatabaseResources().FileSystem(databaseUuid, branchUuid),
		id:             uuid.NewString(),
		statements:     sync.Map{},
		tempFileSystem: tempFileSystem,
	}

	err = con.RegisterVFS()

	if err != nil {
		log.Println("Error Registering VFS:", err)

		return nil, err
	}

	con.setAuthorizer()

	path, err := file.GetDatabaseFileTmpPath(node.Node().Id, databaseUuid, branchUuid)

	if err != nil {
		log.Println("Error Getting Database File Path:", err)

		return nil, err
	}

	connection, err = sqlite3.Open(
		con.context,
		path,
		fmt.Sprintf("litebase:%s", con.VfsHash()),
		sqlite3.SQLITE_OPEN_CREATE|sqlite3.SQLITE_OPEN_READWRITE|sqlite3.SQLITE_OPEN_NOMUTEX,
	)

	if err != nil {
		return nil, err
	}

	con.sqlite3 = connection
	_, err = con.sqlite3.Exec(ctx, fmt.Sprintf("PRAGMA page_size = %d", config.Get().PageSize))

	if err != nil {
		return nil, err
	}
	// If we are not using the original vfs, we do not need this
	con.sqlite3.Exec(ctx, "PRAGMA synchronous=OFF")
	_, err = con.sqlite3.Exec(ctx, "PRAGMA journal_mode=wal")

	if err != nil {
		return nil, err
	}

	_, err = con.sqlite3.Exec(ctx, "PRAGMA busy_timeout = 3000")

	if err != nil {
		return nil, err
	}

	_, err = con.sqlite3.Exec(ctx, "PRAGMA cache_size = -2000000") // 2GB
	// _, err = con.sqlite3.Exec(ctx, "PRAGMA cache_size = 0")

	if err != nil {
		return nil, err
	}

	_, err = con.sqlite3.Exec(ctx, "PRAGMA secure_delete = true")

	if err != nil {
		return nil, err
	}

	// VFS does not handle temp files yet, so we will handle in memory.
	_, err = con.sqlite3.Exec(ctx, "PRAGMA temp_store = memory")

	if err != nil {
		return nil, err
	}

	if !node.Node().IsPrimary() {
		_, err = con.sqlite3.Exec(ctx, "PRAGMA query_only = true")
	}

	return con, err
}

func (con *DatabaseConnection) Changes() int64 {
	return con.sqlite3.Changes()
}

func (con *DatabaseConnection) Checkpoint() error {
	if con == nil || con.sqlite3 == nil {
		return nil
	}

	if con.checkpointer.Running() {
		return nil
	}

	// TODO: What if the checkpoint takes too long? Will other checkpoints be blocked?
	// Will this also block the main thread?
	_, err := sqlite3.Checkpoint(con.sqlite3.Base(), func(result sqlite3.CheckpointResult) {
		err := con.checkpointer.Run()

		if err != nil {
			log.Println("Error checkpointing database", err)
		}
	})

	return err
}

func (con *DatabaseConnection) Close() {
	// Cancel the context of the connection.
	con.cancel()

	// Ensure all statements are finalized before closing the connection.
	con.statements.Range(func(key any, statement any) bool {
		statement.(Statement).Sqlite3Statement.Finalize()

		return true
	})

	con.statements = sync.Map{}

	if con.sqlite3 != nil {
		con.sqlite3.Close()
	}

	vfs.UnregisterVFS(
		fmt.Sprintf("litebase:%s", con.databaseHash),
		fmt.Sprintf("litebase:%s", con.VfsHash()),
	)

	con.sqlite3 = nil
}

func (con *DatabaseConnection) Closed() bool {
	return con.sqlite3 == nil
}

func (con *DatabaseConnection) Commit() error {
	commitStatemnt, err := con.Statement("COMMIT")

	if err != nil {
		return err
	}

	return con.SqliteConnection().Committing(func() error {
		_, err = commitStatemnt.Sqlite3Statement.Exec()

		return err
	})
}

func (con *DatabaseConnection) Context() context.Context {
	return con.context
}

func (c *DatabaseConnection) Id() string {
	return c.id
}

func (con *DatabaseConnection) Prepare(ctx context.Context, command string) (Statement, error) {
	statment, err := con.sqlite3.Prepare(ctx, command)

	if err != nil {
		return Statement{}, err
	}

	return Statement{
		context:          ctx,
		Sqlite3Statement: statment,
	}, nil
}

func (con *DatabaseConnection) Query(statement *sqlite3.Statement, parameters ...any) (sqlite3.Result, error) {
	return con.Transaction(
		statement.IsReadonly(),
		func(con *DatabaseConnection) (sqlite3.Result, error) {
			result, err := statement.Exec(parameters...)

			if err != nil {
				return sqlite3.Result{}, err
			}

			return result, nil
		})
}

func (con *DatabaseConnection) Statement(queryStatement string) (Statement, error) {
	var err error

	hash := crc32.ChecksumIEEE([]byte(queryStatement))

	statement, ok := con.statements.Load(hash)

	if !ok {
		statement, err = con.Prepare(con.context, queryStatement)

		if err == nil {
			// TODO: If the schema changes, the statement will be invalid.
			// We should track if a Query performs DDL and invalidate the
			// statement cache for each connection.
			con.statements.Store(hash, statement)
		}
	}

	return statement.(Statement), err
}

func (con *DatabaseConnection) SqliteConnection() *sqlite3.Connection {
	return con.sqlite3
}

func (c *DatabaseConnection) setAuthorizer() {
	if c.accessKey.AccessKeyId == "" {
		return
	}

	c.sqlite3.Authorizer(func(actionCode int, arg1, arg2, arg3, arg4 string) int {
		allowed := true
		var err error

		args := []string{arg1, arg2, arg3, arg4}

		switch actionCode {
		case sqlite3.SQLITE_COPY:
			allowed = false
		case sqlite3.SQLITE_CREATE_INDEX:
			allowed, err = c.accessKey.CanIndex(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_TABLE:
			allowed, err = c.accessKey.CanCreate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_TEMP_INDEX:
			allowed, err = c.accessKey.CanIndex(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_TEMP_TABLE:
			allowed, err = c.accessKey.CanCreate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_TEMP_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_TEMP_VIEW:
			allowed, err = c.accessKey.CanCreate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_CREATE_VIEW:
			allowed, err = c.accessKey.CanCreate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DELETE:
			allowed, err = c.accessKey.CanDelete(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_INDEX:
			allowed, err = c.accessKey.CanIndex(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_TABLE:
			allowed, err = c.accessKey.CanDrop(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_TEMP_INDEX:
			allowed, err = c.accessKey.CanIndex(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_TEMP_TABLE:
			allowed, err = c.accessKey.CanDrop(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_TEMP_TRIGGER:
			allowed, err = c.accessKey.CanDrop(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_TEMP_VIEW:
			allowed, err = c.accessKey.CanDrop(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_VIEW:
			allowed, err = c.accessKey.CanCreate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_INSERT:
			allowed, err = c.accessKey.CanInsert(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_PRAGMA:
			allowed, err = c.accessKey.CanPragma(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_READ:
			allowed, err = c.accessKey.CanRead(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_SELECT:
			allowed, err = c.accessKey.CanSelect(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_TRANSACTION:
			allowed, err = true, nil
		case sqlite3.SQLITE_UPDATE:
			allowed, err = c.accessKey.CanUpdate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_ATTACH:
			allowed, err = false, nil
		case sqlite3.SQLITE_DETACH:
			allowed, err = false, nil
		case sqlite3.SQLITE_REINDEX:
			allowed, err = c.accessKey.CanIndex(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_ANALYZE:
			allowed, err = true, nil
		case sqlite3.SQLITE_CREATE_VTABLE:
			allowed, err = c.accessKey.CanCreate(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_DROP_VTABLE:
			allowed, err = c.accessKey.CanDrop(c.databaseUuid, c.branchUuid, args)
		case sqlite3.SQLITE_FUNCTION:
			allowed, err = true, nil
		default:
			allowed, err = false, nil
		}

		if err != nil {
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

func (con *DatabaseConnection) Transaction(
	readOnly bool,
	handler func(con *DatabaseConnection) (sqlite3.Result, error),
) (sqlite3.Result, error) {
	var err error

	// Based on the readonly state of the transaction, we will lock the vfs to
	// prevent more that one write transaction from happening at the same time.

	if !readOnly {
		// Start the transaction with a write lock.
		err = con.SqliteConnection().BeginImmediate()

		// Writes should only happen on the primary node. So we can adjust the
		// wal timestamp on the connection to the current time.
		// con.walTimestamp = time.Now().UTC().UnixNano()

		// Notify the database file system that a write transaction is happening.
		// con.fileSystem.SetTransactionTimestamp(time.Now().UTC().UnixNano())
	} else {
		err = con.SqliteConnection().BeginDeferred()
	}

	if err != nil {
		log.Println("Transaction Error:", err)
		return sqlite3.Result{}, err
	}

	results, handlerError := handler(con)

	if handlerError != nil {
		log.Println("Transaction Error:", handlerError)
		err = con.Rollback()

		if err != nil {
			log.Println("Transaction Error:", err)
			return sqlite3.Result{}, err
		}

		return sqlite3.Result{}, handlerError
	}

	err = con.Commit()

	if err != nil {
		log.Println("Transaction Error:", err)
		return sqlite3.Result{}, err
	}

	if !readOnly {
		con.committedAt = time.Now()
	}

	return results, handlerError
}

func (con *DatabaseConnection) RegisterVFS() error {
	err := vfs.RegisterVFS(
		fmt.Sprintf("litebase:%s", con.databaseHash),
		fmt.Sprintf("litebase:%s", con.VfsHash()),
		file.GetDatabaseFileDir(con.databaseUuid, con.branchUuid),
		config.Get().PageSize,
		con.fileSystem,
	)

	if err != nil {
		return err
	}

	return nil
}

func (con *DatabaseConnection) Rollback() error {
	commitStatemnt, err := con.Statement("ROLLBACK")

	if err != nil {
		return err
	}

	_, err = commitStatemnt.Sqlite3Statement.Exec()

	return err
}

func (con *DatabaseConnection) VfsHash() string {
	if con.vfsHash == "" {
		sha1 := sha1.New()
		sha1.Write([]byte(fmt.Sprintf("%s:%s:%s", con.databaseUuid, con.branchUuid, con.id)))
		con.vfsHash = fmt.Sprintf("%x", sha1.Sum(nil))
	}

	return con.vfsHash
}

func (con *DatabaseConnection) WithAccessKey(accessKey auth.AccessKey) *DatabaseConnection {
	con.accessKey = accessKey

	return con
}
