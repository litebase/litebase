package database

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/sqlite3"
	"log"

	"github.com/google/uuid"
)

type Connection struct {
	accessKey *auth.AccessKey
	// connectionTransaction *ConnectionTransaction
	id            string
	inTransaction bool
	opened        bool
	Operator      *DatabaseOperator
	Path          string
	sqlite3       *sqlite3.Connection
	statements    map[string]*sqlite3.Statement
	WAL           *DatabaseWAL
}

func NewConnection(path string, accessKey *auth.AccessKey) *Connection {
	wal := NewWAL(path)
	connection := &Connection{
		accessKey:  accessKey,
		Operator:   NewOperator(wal),
		Path:       path,
		statements: map[string]*sqlite3.Statement{},
		WAL:        wal,
	}

	connection.setId()

	return connection
}

func (c *Connection) Begin() bool {
	if c.inTransaction {
		return false
	}

	c.sqlite3.Exec("BEGIN")
	c.inTransaction = true

	return true
}

func (c *Connection) Changes() int64 {
	return c.sqlite3.Changes()
}

func (c *Connection) Close() {
	for key, statement := range c.statements {
		statement.Finalize()
		delete(c.statements, key)
	}

	CloseVFSFiles()

	c.opened = false
	c.sqlite3.Close()
	c.statements = map[string]*sqlite3.Statement{}
	c.inTransaction = false
}

func (c *Connection) Commit() {
	if !c.inTransaction {
		return
	}

	c.sqlite3.Exec("COMMIT")
	c.inTransaction = false
}

func (c *Connection) Id() string {
	return c.id
}

func (c *Connection) IsOpen() bool {
	return c.opened
}

func (c *Connection) LastInsertRowID() int64 {
	return c.sqlite3.LastInsertId()
}

func (c *Connection) Open() error {
	sqlite3Connection, err := sqlite3.Open(fmt.Sprintf("file:%s?mode=rwc&vfs=litebasedb", c.Path), 0, "")

	if err != nil {
		log.Fatal(err)
	}

	c.sqlite3 = sqlite3Connection
	// c.sqlite3.BusyTimeout(3 * time.Second)
	_, err = c.sqlite3.Exec("PRAGMA synchronous = OFF", []interface{}{}...)

	if err != nil {
		log.Fatal(err)
	}

	_, err = c.sqlite3.Exec("PRAGMA journal_mode = OFF;", []interface{}{}...)

	if err != nil {
		log.Fatal(err)
	}

	c.setAuthorizer()

	c.opened = true

	return nil
}

func (c *Connection) Prepare(statement string) (*sqlite3.Statement, error) {
	var err error
	var exists bool
	var sqlite3Statement *sqlite3.Statement

	h := sha1.New()
	io.WriteString(h, statement)
	hash := hex.EncodeToString(h.Sum(nil))

	sqlite3Statement, exists = c.statements[hash]

	if !exists {
		sqlite3Statement, err = c.sqlite3.Prepare(statement)

		if err == nil {
			c.statements[hash] = sqlite3Statement
		}
	}

	return sqlite3Statement, err
}

func (c *Connection) Query(statement *sqlite3.Statement, parameters ...interface{}) (sqlite3.Result, error) {
	return c.Operator.Monitor(
		statement.IsReadonly(),
		func() (sqlite3.Result, error) {
			result, err := statement.Exec(parameters...)

			if err != nil {
				return nil, err
			}

			return result, nil
		})
}

func (c *Connection) Rollback() {
	if !c.inTransaction {
		return
	}

	c.sqlite3.Exec("ROLLBACK")
	c.inTransaction = false
}

func (c *Connection) setAuthorizer() {
	if c.accessKey == nil {
		return
	}

	c.sqlite3.SetAuthorizer(func(actionCode int, arg1, arg2, arg3, arg4 string) int {
		allowed := true
		var err error

		args := []string{arg1, arg2, arg3, arg4}

		switch actionCode {
		case sqlite3.SQLITE_COPY:
			allowed = false
		case sqlite3.SQLITE_CREATE_INDEX:
			allowed, err = c.accessKey.CanIndex(args)
		case sqlite3.SQLITE_CREATE_TABLE:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_CREATE_TEMP_INDEX:
			allowed, err = c.accessKey.CanIndex(args)
		case sqlite3.SQLITE_CREATE_TEMP_TABLE:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_CREATE_TEMP_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(args)
		case sqlite3.SQLITE_CREATE_TEMP_VIEW:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_CREATE_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(args)
		case sqlite3.SQLITE_CREATE_VIEW:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_DELETE:
			allowed, err = c.accessKey.CanDelete(args)
		case sqlite3.SQLITE_DROP_INDEX:
			allowed, err = c.accessKey.CanIndex(args)
		case sqlite3.SQLITE_DROP_TABLE:
			allowed, err = c.accessKey.CanDrop(args)
		case sqlite3.SQLITE_DROP_TEMP_INDEX:
			allowed, err = c.accessKey.CanIndex(args)
		case sqlite3.SQLITE_DROP_TEMP_TABLE:
			allowed, err = c.accessKey.CanDrop(args)
		case sqlite3.SQLITE_DROP_TEMP_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(args)
		case sqlite3.SQLITE_DROP_TEMP_VIEW:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_DROP_TRIGGER:
			allowed, err = c.accessKey.CanTrigger(args)
		case sqlite3.SQLITE_DROP_VIEW:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_INSERT:
			allowed, err = c.accessKey.CanInsert(args)
		case sqlite3.SQLITE_PRAGMA:
			allowed, err = c.accessKey.CanPragma(args)
		case sqlite3.SQLITE_READ:
			allowed, err = c.accessKey.CanRead(args)
		case sqlite3.SQLITE_SELECT:
			allowed, err = c.accessKey.CanSelect(args)
		case sqlite3.SQLITE_TRANSACTION:
			allowed, err = true, nil
		case sqlite3.SQLITE_UPDATE:
			allowed, err = c.accessKey.CanUpdate(args)
		case sqlite3.SQLITE_ATTACH:
			allowed, err = false, nil
		case sqlite3.SQLITE_DETACH:
			allowed, err = false, nil
		case sqlite3.SQLITE_REINDEX:
			allowed, err = c.accessKey.CanIndex(args)
		case sqlite3.SQLITE_ANALYZE:
			allowed, err = true, nil
		case sqlite3.SQLITE_CREATE_VTABLE:
			allowed, err = c.accessKey.CanCreate(args)
		case sqlite3.SQLITE_DROP_VTABLE:
			allowed, err = c.accessKey.CanDrop(args)
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

func (c *Connection) setId() {
	c.id = uuid.New().String()
}
