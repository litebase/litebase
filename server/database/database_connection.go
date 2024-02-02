package database

import (
	"fmt"
	"hash/crc32"
	"log"
	"sync"

	"litebasedb/server/auth"
	"litebasedb/server/sqlite3"
	"litebasedb/server/vfs"

	"github.com/google/uuid"
)

var vfsRegistered = false
var vfsMutex = &sync.Mutex{}

type DatabaseConnection struct {
	accessKey    *auth.AccessKey
	branchUuid   string
	connection   *sqlite3.Connection
	databaseUuid string
	fileSystem   *FileSystem
	id           string
	mutex        *sync.Mutex
	path         string
	session      *sqlite3.Session
	statements   map[uint32]*sqlite3.Statement
}

func NewDatabaseConnection(path, databaseUuid, branchUuid string) *DatabaseConnection {
	var (
		connection *sqlite3.Connection
		err        error
	)

	con := &DatabaseConnection{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
		fileSystem:   NewFileSystem(path),
		// accessKey:  accessKey,
		mutex:      &sync.Mutex{},
		path:       path,
		statements: map[uint32]*sqlite3.Statement{},
	}

	con.setId()
	con.RegisterVFS()
	// TODO: add authorizer, something is broken
	con.setAuthorizer()

	connection, err = sqlite3.Open(fmt.Sprintf("litebase:%s", con.id), 0)

	if err != nil {
		log.Fatalln("Erorr init", err)
	}

	con.connection = connection

	con.connection.Exec("PRAGMA synchronous=OFF")
	con.connection.Exec("pragma journal_mode=OFF")
	// con.connection.Exec("pragma journal_mode=WAL")
	con.connection.Exec("pragma wal_autocheckpoint=100")
	con.connection.Exec("PRAGMA cache_size = -125000") // 125K Kib = 128MB
	con.connection.Exec("PRAGMA busy_timeout = 3000")

	return con
}

func (con *DatabaseConnection) Changes() int64 {
	return con.connection.Changes()
}

func (con *DatabaseConnection) Close() {
	log.Println("Closing connection")
	// con.connection.Close()
	con.fileSystem.CheckPoint()
	// con.file.Close()
	con.connection = nil
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
	var exists bool
	var statement *sqlite3.Statement

	// hash := md5.Sum([]byte(queryStatement))
	hash := crc32.ChecksumIEEE([]byte(queryStatement))

	con.mutex.Lock()
	statement, exists = con.statements[hash]
	con.mutex.Unlock()

	if !exists {
		statement, err = con.connection.Prepare(queryStatement)

		if err == nil {
			// TODO: If the schema changes, the statement will be invalid.
			// We should track if a Query performs DDL and invalidate the
			// statement cache for each connection.
			con.mutex.Lock()
			con.statements[hash] = statement
			con.mutex.Unlock()
		}
	}

	return statement, err
}

func (con *DatabaseConnection) SqliteConnection() *sqlite3.Connection {
	return con.connection
}

func (con *DatabaseConnection) SessionStart() error {
	if con.session != nil {
		return fmt.Errorf("session already in progress")
	}

	con.session = sqlite3.CreateSession(con.connection.Base())

	return nil
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
	if !readOnly {
		unlock := con.Lock(readOnly)
		defer unlock()

		_, err = con.SqliteConnection().Exec("BEGIN")

		if err != nil {
			log.Println("Transaction Error:", err)
			return nil, err
		}
	}

	results, handlerError := handler(con)

	if !readOnly {
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
	}

	return results, handlerError
}

func (con *DatabaseConnection) RegisterVFS() {
	vfsMutex.Lock()
	defer vfsMutex.Unlock()

	if vfsRegistered {
		return
	}

	err := vfs.RegisterVFS(con.fileSystem)

	if err != nil {
		log.Fatalf("Register VFS err: %s", err)
	}

	vfsRegistered = true
}

func (con *DatabaseConnection) Lock(readOnly bool) func() {
	// if readOnly {
	// 	ConnectionManager().GetMutex(con.databaseUuid, con.branchUuid).RLock()

	// 	return func() {
	// 		ConnectionManager().GetMutex(con.databaseUuid, con.branchUuid).RUnlock()
	// 	}
	// } else {
	ConnectionManager().GetMutex(con.databaseUuid, con.branchUuid).Lock()

	return func() {
		ConnectionManager().GetMutex(con.databaseUuid, con.branchUuid).Unlock()
	}
	// }
}

func (con *DatabaseConnection) WithAccessKey(accessKey *auth.AccessKey) *DatabaseConnection {
	con.accessKey = accessKey

	return con
}
