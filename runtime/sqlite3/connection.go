package sqlite3

import (
	"errors"
	"time"
	"unsafe"

	"github.com/google/uuid"
)

/*
#include <sqlite3.h>
#include <stdlib.h>

extern int go_authorizer(void* pArg, int actionCode, char* arg1, char* arg2, char* arg3, char* arg4);
extern void go_commit_hook();
*/
import "C"

type OpenFlags C.int
type Connection struct {
	authorizerCallback Authorizer
	Id                 string
	SQLite3Connection  *C.sqlite3
}

var connections = make(map[string]*Connection)

type Authorizer func(action int, arg1, arg2, arg3, arg4 string) int

var authorizerCallback Authorizer
var commitHook func() (abort bool)

func init() {
	if err := C.sqlite3_initialize(); err != C.SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errstr(err))))
	}
}

func Open(path string, flags OpenFlags, vfs string) (*Connection, error) {
	var cVfs, cName *C.char
	var c *C.sqlite3

	cName = C.CString(path)
	defer C.free(unsafe.Pointer(cName))

	// Set memory database if empty string
	if path == "" || path == ":memory:" {
		path = ":memory:"
		flags |= C.SQLITE_OPEN_MEMORY
	}

	// Set flags, add read/write flag if create flag is set
	if flags == 0 {
		flags = C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_URI
	}

	if flags|C.SQLITE_OPEN_CREATE > 0 {
		flags |= C.SQLITE_OPEN_READWRITE
	}

	// Call sqlite3_open_v2
	if err := C.sqlite3_open_v2(cName, &c, C.int(flags), cVfs); err != C.SQLITE_OK {
		if c != nil {
			C.sqlite3_close_v2(c)
		}
		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Set extended error codes
	// if err := C.sqlite3_extended_result_codes(c, 1); err != C.SQLITE_OK {
	// 	C.sqlite3_close_v2(c)
	// 	return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	// }

	// Initialize sqlite
	if err := C.sqlite3_initialize(); err != C.SQLITE_OK {
		C.sqlite3_close_v2(c)
		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	connection := &Connection{
		Id:                uuid.NewString(),
		SQLite3Connection: c,
	}

	connections[connection.Id] = connection

	return connections[connection.Id], nil
}

// Prepare query
func (c *Connection) Prepare(query string) (*Statement, error) {
	var cQuery, cExtra *C.char
	var s *C.sqlite3_stmt

	cQuery = C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if err := C.sqlite3_prepare_v2((*C.sqlite3)(c.SQLite3Connection), cQuery, -1, &s, &cExtra); err != C.SQLITE_OK {
		return nil, c.Error(err)
	}

	// Return prepared statement and extra string
	return &Statement{c, s, C.GoString(cExtra)}, nil
}

// Execute a query
func (c *Connection) Exec(query string, params ...interface{}) (Result, error) {
	var stmt *Statement
	var err error

	if stmt, err = c.Prepare(query); err != nil {
		return nil, err
	}

	defer stmt.Finalize()

	return stmt.Exec(params...)
}

// Close Connection
func (c *Connection) Close() error {
	var result error
	delete(connections, c.Id)

	// Close any active statements
	/*var s *Statement
	for {
		s = c.NextStatement(s)
		if s == nil {
			break
		}
		fmt.Println("finalizing", uintptr(unsafe.Pointer(s)))
		if err := s.Finalize(); err != nil {
			result = multierror.Append(result, err)
		}
	}*/

	if err := C.sqlite3_close_v2((*C.sqlite3)(c.SQLite3Connection)); err != C.SQLITE_OK {
		result = errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	return result
}

// Get Read-only state. Also returns false if database not found
func (c *Connection) Readonly(schema string) bool {
	var cSchema *C.char

	// Set schema to default if empty string
	if schema == "" {
		schema = "main"
	}

	cSchema = C.CString(schema)
	defer C.free(unsafe.Pointer(cSchema))

	r := int(C.sqlite3_db_readonly((*C.sqlite3)(c.SQLite3Connection), cSchema))

	if r == -1 {
		return false
	} else {
		return r != 0
	}
}

// Set the busy timeout for the connection
func (c *Connection) BusyTimeout(duration time.Duration) error {
	if err := C.sqlite3_busy_timeout((*C.sqlite3)(c.SQLite3Connection), C.int(duration/time.Millisecond)); err != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(err)))
	} else {
		return nil
	}
}

// Get number of rows affected by last query
func (c *Connection) Changes() int64 {
	return int64(C.sqlite3_changes((*C.sqlite3)(c.SQLite3Connection)))
}

// Get last insert id
func (c *Connection) LastInsertRowID() int64 {
	return int64(C.sqlite3_last_insert_rowid((*C.sqlite3)(c.SQLite3Connection)))
}

// Cache Flush
func (c *Connection) CacheFlush() error {
	if err := C.sqlite3_db_cacheflush((*C.sqlite3)(c.SQLite3Connection)); err != C.SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(err)))
	} else {
		return nil
	}
}

// Get last insert id
func (c *Connection) LastInsertId() int64 {
	return int64(C.sqlite3_last_insert_rowid((*C.sqlite3)(c.SQLite3Connection)))
}

// Set last insert id
func (c *Connection) SetLastInsertId(v int64) {
	C.sqlite3_set_last_insert_rowid((*C.sqlite3)(c.SQLite3Connection), C.sqlite3_int64(v))
}

// Interrupt all queries for connection
func (c *Connection) Interrupt() {
	C.sqlite3_interrupt((*C.sqlite3)(c.SQLite3Connection))
}

// Register a Go function as a commit hook on the SQLite database connection.
// The hook function will be called whenever a transaction is committed.
// The hook function should return true to rollback the transaction or false to commit the transaction.
func (c *Connection) CommitHook(hook func() (abort bool)) {
	commitHook = hook

	C.sqlite3_commit_hook((*C.sqlite3)(c.SQLite3Connection), (*[0]byte)(C.go_commit_hook), nil)
}

//export go_commit_hook
func go_commit_hook() {
	if commitHook != nil {
		commitHook()
	}
}

// Register a Go function as an authorizer callback function.
// https://www.sqlite.org/c3ref/set_authorizer.html
func (c *Connection) SetAuthorizer(authorizer Authorizer) {
	c.authorizerCallback = authorizer

	userInfo := unsafe.Pointer(C.CString(c.Id))

	C.sqlite3_set_authorizer((*C.sqlite3)(c.SQLite3Connection), (*[0]byte)(C.go_authorizer), userInfo)
}

//export go_authorizer
func go_authorizer(userInfo unsafe.Pointer, action C.int, arg1, arg2, arg3, arg4 *C.char) C.int {
	connectionId := C.GoString((*C.char)(userInfo))
	c := connections[connectionId]

	if c != nil {
		return C.int(c.authorizerCallback(int(action), C.GoString(arg1), C.GoString(arg2), C.GoString(arg3), C.GoString(arg4)))
	}

	return C.int(0)
}
