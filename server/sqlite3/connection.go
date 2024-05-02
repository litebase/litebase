package sqlite3

import (
	"errors"
	"log"
	"time"
	"unsafe"
)

/*
#include "./sqlite3.h"
#include <stdlib.h>

extern int go_authorizer(void* pArg, int actionCode, char* arg1, char* arg2, char* arg3, char* arg4);
extern void go_commit_hook();
*/
import "C"

type OpenFlags C.int
type Connection C.sqlite3

type Authorizer func(action int, arg1, arg2, dbName, triggerOrView string) (allow int)

var authorizerCallback Authorizer
var commitHook func() (abort bool)

func init() {
	if err := C.sqlite3_initialize(); err != SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errstr(err))))
	}
}

func (c *Connection) Base() *C.sqlite3 {
	return (*C.sqlite3)(c)
}

func Open(path, vfsId string) (*Connection, error) {
	var cVfs, cName *C.char
	var c *C.sqlite3

	// Set vfs
	cVfs = C.CString(vfsId)
	defer C.free(unsafe.Pointer(cVfs))

	cName = C.CString(path)
	defer C.free(unsafe.Pointer(cName))

	// Set flags, add read/write flag if create flag is set
	flags := SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE

	// Call sqlite3_open_v2
	if err := C.sqlite3_open_v2(cName, &c, C.int(flags), cVfs); err != SQLITE_OK {
		if c != nil {
			C.sqlite3_close_v2(c)
		}

		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Set extended error codes
	if err := C.sqlite3_extended_result_codes(c, 1); err != SQLITE_OK {
		C.sqlite3_close_v2(c)
		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Initialize sqlite
	if err := C.sqlite3_initialize(); err != SQLITE_OK {
		C.sqlite3_close_v2(c)
		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	return (*Connection)(c), nil
}

// Execute a query
func (c *Connection) Exec(query string, params ...interface{}) (Result, error) {
	var stmt *Statement
	var err error

	if stmt, err = c.Prepare(query); err != nil {
		return nil, err
	}

	defer func() {
		err := stmt.Finalize()

		if err != nil {
			log.Fatalln("Error finalizing statement:", err)
		}
	}()

	return stmt.Exec(params...)
}

// Close Connection
func (c *Connection) Close() error {
	var result error

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

	// Close database connection
	if err := C.sqlite3_close_v2((*C.sqlite3)(c)); err != SQLITE_OK {
		result = errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Return any errors
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

	r := int(C.sqlite3_db_readonly((*C.sqlite3)(c), cSchema))

	if r == -1 {
		return false
	} else {
		return r != 0
	}
}

// Set the busy timeout for the connection
func (c *Connection) BusyTimeout(duration time.Duration) error {
	if err := C.sqlite3_busy_timeout((*C.sqlite3)(c), C.int(duration/time.Millisecond)); err != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(err)))
	} else {
		return nil
	}
}

// Get number of rows affected by last query
func (c *Connection) Changes() int64 {
	return int64(C.sqlite3_changes((*C.sqlite3)(c)))
}

// Get last insert id
func (c *Connection) LastInsertRowID() int64 {
	return int64(C.sqlite3_last_insert_rowid((*C.sqlite3)(c)))
}

// Cache Flush
func (c *Connection) CacheFlush() error {
	if err := C.sqlite3_db_cacheflush((*C.sqlite3)(c)); err != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(err)))
	} else {
		return nil
	}
}

// Get last insert id
func (c *Connection) LastInsertId() int64 {
	return int64(C.sqlite3_last_insert_rowid((*C.sqlite3)(c)))
}

// Set last insert id
func (c *Connection) SetLastInsertId(v int64) {
	C.sqlite3_set_last_insert_rowid((*C.sqlite3)(c), C.sqlite3_int64(v))
}

// Interrupt all queries for connection
func (c *Connection) Interrupt() {
	C.sqlite3_interrupt((*C.sqlite3)(c))
}

// Register a Go function as a commit hook on the SQLite database connection.
// The hook function will be called whenever a transaction is committed.
// The hook function should return true to rollback the transaction or false to commit the transaction.
func (c *Connection) CommitHook(hook func() (abort bool)) {
	commitHook = hook

	C.sqlite3_commit_hook((*C.sqlite3)(c), (*[0]byte)(C.go_commit_hook), nil)
}

//export go_commit_hook
func go_commit_hook() {
	if commitHook != nil {
		commitHook()
	}
}

// Register a Go function as an authorizer callback function.
// https://www.sqlite.org/c3ref/set_authorizer.html
func (c *Connection) Authorizer(authorizer Authorizer) {
	authorizerCallback = authorizer

	C.sqlite3_set_authorizer((*C.sqlite3)(c), (*[0]byte)(C.go_authorizer), nil)
}

//export go_authorizer
func go_authorizer(userInfo unsafe.Pointer, action C.int, arg1, arg2, dbName, triggerOrView *C.char) C.int {
	// if authorizerCallback != nil {
	// 	return C.int(authorizerCallback(int(action), C.GoString(arg1), C.GoString(arg2), C.GoString(dbName), C.GoString(triggerOrView)))
	// }

	return C.int(0)
}
