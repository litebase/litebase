package database

import (
	"crypto/sha1"
	"fmt"
	"hash/crc32"
	"log"
	"sync"
	"time"

	"litebasedb/server/auth"
	"litebasedb/server/file"
	"litebasedb/server/sqlite3"
	"litebasedb/server/storage"
	"litebasedb/server/vfs"

	"github.com/google/uuid"
)

type DatabaseConnection struct {
	accessKey      *auth.AccessKey
	branchUuid     string
	commitedAt     time.Time
	connection     *sqlite3.Connection
	databaseUuid   string
	id             string
	fileSystem     *storage.LocalDatabaseFileSystem
	openRetries    int
	statements     map[uint32]*sqlite3.Statement
	statementMutex sync.RWMutex
	tempFileSystem *storage.LocalDatabaseFileSystem
	vfs            *vfs.LitebaseVFS
}

func NewDatabaseConnection(databaseUuid, branchUuid string) *DatabaseConnection {
	log.Println("Opening connection")
	var (
		connection *sqlite3.Connection
		err        error
	)

	con := &DatabaseConnection{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
		fileSystem: storage.NewLocalDatabaseFileSystem(
			fmt.Sprintf("%s/%s/%s", Directory(), databaseUuid, branchUuid),
			databaseUuid,
			branchUuid,
		),
		statements:     map[uint32]*sqlite3.Statement{},
		statementMutex: sync.RWMutex{},
		tempFileSystem: storage.NewLocalDatabaseFileSystem(
			fmt.Sprintf("%s/%s/%s", Directory(), databaseUuid, branchUuid),
			databaseUuid,
			branchUuid,
		),
	}

	con.setId()
	con.RegisterVFS()
	con.setAuthorizer()

	con.openRetries = 0

open:
	connection, err = sqlite3.Open(
		fmt.Sprintf("./%s/%s.db", file.GetFileDir(databaseUuid, branchUuid), con.Hash()),
		fmt.Sprintf("litebase:%s", con.VfsHash()),
	)

	if err != nil {
		log.Println("Error Opening Database:", err)

		if con.openRetries < 3 {
			con.openRetries++
			randomWait := time.Duration(con.openRetries*100) * time.Millisecond
			time.Sleep(randomWait)
			goto open
		}

		return nil
	}

	con.connection = connection

	con.connection.Exec("PRAGMA synchronous=OFF")
	// con.connection.Exec("PRAGMA synchronous=NORMAL")
	// con.connection.Exec("PRAGMA journal_mode=delete")
	con.connection.Exec("PRAGMA journal_mode=wal")
	// TODO: Need to figure out how to allow checkpoints to resu3me once there
	// is more than one connection to the database opened. See the documentation
	// https://www.sqlite.org/wal.html#avoiding_excessively_large_wal_files
	con.connection.Exec("PRAGMA wal_autocheckpoint=1000")
	con.connection.Exec("PRAGMA busy_timeout = 3000")
	// con.connection.Exec("PRAGMA cache_size = 0")
	con.connection.Exec("PRAGMA secure_delete = true")
	// con.connection.Exec("PRAGMA temp_store = memory")
	// con.connection.Exec("PRAGMA mmap_size = 30000000000")

	// con.checkpointer = NewCheckpointer(func() {
	// 	con.CheckPoint()
	// })

	return con
}

func (con *DatabaseConnection) Changes() int64 {
	return con.connection.Changes()
}

func (con *DatabaseConnection) Checkpoint() error {
	_, err := sqlite3.Checkpoint(con.connection.Base())

	if err != nil {
		log.Println("Checkpoint Error:", err)
	}

	return err
}

func (con *DatabaseConnection) Close() {
	con.connection.Close()
	vfs.UnregisterVFS(
		fmt.Sprintf("litebase:%s", con.Hash()),
		fmt.Sprintf("litebase:%s", con.VfsHash()),
	)
	con.connection = nil
	con.vfs = nil
}

func (con *DatabaseConnection) Closed() bool {
	return con.connection == nil
}

func (con *DatabaseConnection) Hash() string {
	sha1 := sha1.New()
	sha1.Write([]byte(fmt.Sprintf("%s:%s", con.databaseUuid, con.branchUuid)))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func (c *DatabaseConnection) Id() string {
	return c.id
}

func (con *DatabaseConnection) Query(statement *sqlite3.Statement, parameters ...interface{}) (sqlite3.Result, error) {
	return con.Transaction(
		statement.IsReadonly(),
		func(con *DatabaseConnection) (sqlite3.Result, error) {
			result, err := statement.Exec(parameters...)

			if err != nil {
				return nil, err
			}

			return result, nil
		})
}

func (con *DatabaseConnection) Statement(queryStatement string) (*sqlite3.Statement, error) {
	var err error

	hash := crc32.ChecksumIEEE([]byte(queryStatement))

	con.statementMutex.RLock()
	statement, ok := con.statements[hash]
	con.statementMutex.RUnlock()

	if !ok {
		statement, err = con.connection.Prepare(queryStatement)

		if err == nil {
			// TODO: If the schema changes, the statement will be invalid.
			// We should track if a Query performs DDL and invalidate the
			// statement cache for each connection.
			con.statementMutex.Lock()
			con.statements[hash] = statement
			con.statementMutex.Unlock()
		}
	}

	return statement, err
}

func (con *DatabaseConnection) SqliteConnection() *sqlite3.Connection {
	return con.connection
}

func (c *DatabaseConnection) setAuthorizer() {
	if c.accessKey == nil {
		return
	}

	c.connection.Authorizer(func(actionCode int, arg1, arg2, arg3, arg4 string) int {
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

func (c *DatabaseConnection) setId() {
	c.id = uuid.New().String()
}

func (con *DatabaseConnection) Transaction(
	readOnly bool,
	handler func(con *DatabaseConnection) (sqlite3.Result, error),
) (sqlite3.Result, error) {
	var err error

	// Based on the readonly state of the transaction, we will lock the vfs to
	// prevent more that one write transaction from happening at the same time.
	lock := ConnectionManager().GetLock(con.databaseUuid, con.branchUuid)

	if !readOnly {
		lock.Lock()
		defer lock.Unlock()
		_, err = con.SqliteConnection().Exec("BEGIN IMMEDIATE")
	} else {
		// lock.RLock()
		// defer lock.RUnlock()
		_, err = con.SqliteConnection().Exec("BEGIN DEFERRED")
	}

	// unlock := con.Lock(readOnly)
	// defer unlock()

	if err != nil {
		log.Println("Transaction Error:", err)
		return nil, err
	}
	// }

	results, handlerError := handler(con)

	// if !readOnly {
	if handlerError != nil {
		_, err = con.SqliteConnection().Exec("ROLLBACK")

		if err != nil {
			log.Println("Transaction Error:", err)
			return nil, err
		}

		return nil, handlerError
	}

	_, err = con.SqliteConnection().Exec("COMMIT")

	if err != nil {
		log.Println("Transaction Error:", err)
		return nil, err
	}

	if !readOnly {
		con.commitedAt = time.Now()
	}

	return results, handlerError
}

func (con *DatabaseConnection) RegisterVFS() {
	vfs, err := vfs.RegisterVFS(
		fmt.Sprintf("litebase:%s", con.Hash()),
		fmt.Sprintf("litebase:%s", con.VfsHash()),
		con.fileSystem,
		con.tempFileSystem,
	)

	if err != nil {
		log.Fatalf("Register VFS err: %s", err)
	}

	con.vfs = vfs
}

func (con *DatabaseConnection) VfsHash() string {
	sha1 := sha1.New()
	sha1.Write([]byte(fmt.Sprintf("%s:%s:%s", con.databaseUuid, con.branchUuid, con.id)))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func (con *DatabaseConnection) WithAccessKey(accessKey *auth.AccessKey) *DatabaseConnection {
	con.accessKey = accessKey

	return con
}
