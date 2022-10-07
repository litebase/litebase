package database

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/sqlite3"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/psanford/sqlite3vfs"
)

type Connection struct {
	accessKey             *auth.AccessKey
	connectionTransaction *ConnectionTransaction
	id                    string
	inTransaction         bool
	opened                bool
	sqlite3               *sqlite3.Connection
	statements            map[string]*sqlite3.Statement
}

func NewConnection(path string, accessKey *auth.AccessKey) *Connection {
	connection := &Connection{
		accessKey: accessKey,
	}

	connection.SetId()

	sqlite3Connection, err := sqlite3.Open(fmt.Sprintf("file:%s?mode=rwc&vfs=litebasedb", path), sqlite3.OpenFlags(sqlite3vfs.OpenReadWrite|sqlite3vfs.OpenURI), "")

	if err != nil {
		log.Fatal(err)
	}

	connection.sqlite3 = sqlite3Connection
	connection.sqlite3.BusyTimeout(3 * time.Second)
	connection.sqlite3.Exec("PRAGMA synchronous = OFF")
	connection.sqlite3.Exec("PRAGMA journal_mode = OFF")

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

func (c *Connection) Query(statement string, parameters ...interface{}) (sqlite3.Result, error) {
	sqlite3Statement, err := c.Prepare(statement)

	if err != nil {
		return nil, err
	}

	return Operator.Monitor(
		sqlite3Statement.IsReadonly(),
		func() (sqlite3.Result, error) {
			result, err := sqlite3Statement.Exec(parameters...)

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

	c.sqlite3.Authorizer(func(actionCode int, arg1 string, arg2 string, dbName string, triggerOrViewName string) int {
		allowed := true

		switch actionCode {
		case sqlite3.SQLITE_COPY:
		}

		if actionCode == sqlite3.SQLITE_SELECT && !allowed {
			return sqlite3.SQLITE_IGNORE
		}

		return 0
	})
}

func (c *Connection) SetId() {
	c.id = uuid.New().String()
}
