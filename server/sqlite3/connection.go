package sqlite3

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/cgo"
	"time"
	"unsafe"

	"github.com/litebase/litebase/server/constants"
)

/*
#include "./sqlite3.h"
#include <stdlib.h>
#include <stdint.h>

extern int go_authorizer(uintptr_t connectionHandle, int actionCode, char* arg1, char* arg2, char* arg3, char* arg4);
extern int go_progress_handler(uintptr_t connectionHandle);
*/
import "C"

var (
	SQL_BEGIN           = (*C.char)(unsafe.Pointer(C.CString("BEGIN")))
	SQL_BEGIN_DEFERRED  = (*C.char)(unsafe.Pointer(C.CString("BEGIN DEFERRED")))
	SQL_BEGIN_EXCLUSIVE = (*C.char)(unsafe.Pointer(C.CString("BEGIN EXCLUSIVE")))
	SQL_BEGIN_IMMEDIATE = (*C.char)(unsafe.Pointer(C.CString("BEGIN IMMEDIATE")))
	SQL_COMMIT          = (*C.char)(unsafe.Pointer(C.CString("COMMIT")))
	SQL_ROLLBACK        = (*C.char)(unsafe.Pointer(C.CString("ROLLBACK")))
	SQL_VACUUM          = (*C.char)(unsafe.Pointer(C.CString("VACUUM")))
)

type OpenFlags C.int
type Connection struct {
	authorizer         Authorizer
	authorizationError error
	cName              *C.char
	context            context.Context
	cVfs               *C.char
	id                 string
	sqlite3            *C.sqlite3
}

type Authorizer func(action int, arg1, arg2, dbName, triggerOrView string) (allow int)

func init() {
	if err := C.sqlite3_initialize(); err != SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errstr(err))))
	}
}

func (c *Connection) Base() *C.sqlite3 {
	return (*C.sqlite3)(c.sqlite3)
}

func Open(ctx context.Context, path, vfsId string, flags OpenFlags) (*Connection, error) {
	c := &Connection{
		cName:   C.CString(path),
		context: ctx,
		cVfs:    C.CString(vfsId),
		id:      vfsId,
	}

	var vfs *C.char

	if len(vfsId) == 0 {
		vfs = nil
	} else {
		vfs = c.cVfs
	}

	// Call sqlite3_open_v2
	if err := C.sqlite3_open_v2(c.cName, &c.sqlite3, C.int(flags), vfs); err != SQLITE_OK {
		if c.sqlite3 != nil {
			C.sqlite3_close_v2(c.sqlite3)
		}

		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Set extended error codes
	if err := C.sqlite3_extended_result_codes(c.sqlite3, 1); err != SQLITE_OK {
		C.sqlite3_close_v2(c.sqlite3)

		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Initialize sqlite
	if err := C.sqlite3_initialize(); err != SQLITE_OK {
		C.sqlite3_close_v2(c.sqlite3)

		return nil, errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	return c, nil
}

func (c *Connection) Begin() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_BEGIN, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (c *Connection) BeginDeferred() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_BEGIN_DEFERRED, nil, nil, nil)

	if rc != SQLITE_OK {
		return c.Error(int(rc))
	}

	return nil
}

func (c *Connection) BeginExclusive() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_BEGIN_EXCLUSIVE, nil, nil, nil)

	if rc != SQLITE_OK {
		return c.Error(int(rc))
	}

	return nil
}

func (c *Connection) BeginImmediate() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_BEGIN_IMMEDIATE, nil, nil, nil)

	if rc != SQLITE_OK {
		return c.Error(int(rc))
	}

	return nil
}

// Set the busy timeout for the connection
func (c *Connection) BusyTimeout(duration time.Duration) error {
	if err := C.sqlite3_busy_timeout((*C.sqlite3)(c.sqlite3), C.int(duration/time.Millisecond)); err != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(err)))
	} else {
		return nil
	}
}

// Cache Flush
func (c *Connection) CacheFlush() error {
	if err := C.sqlite3_db_cacheflush((*C.sqlite3)(c.sqlite3)); err != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(err)))
	} else {
		return nil
	}
}

// Get number of rows affected by last query
func (c *Connection) Changes() int64 {
	return int64(C.sqlite3_changes((*C.sqlite3)(c.sqlite3)))
}

func (c *Connection) ClearCache() {
	C.sqlite3_file_control((*C.sqlite3)(c.sqlite3), nil, C.SQLITE_FCNTL_RESET_CACHE, nil)
}

// Close Connection
func (c *Connection) Close() error {
	var result error

	if c.sqlite3 == nil {
		return nil
	}

	// Close database connection
	if err := C.sqlite3_close_v2((*C.sqlite3)(c.sqlite3)); err != SQLITE_OK {
		result = errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	C.free(unsafe.Pointer(c.cName))
	C.free(unsafe.Pointer(c.cVfs))

	return result
}

func (c *Connection) Commit() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_COMMIT, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (c *Connection) Error(code int) error {
	if code >= 10000 {
		return constants.ErrorFromCode(code)
	}

	message := C.GoString(C.sqlite3_errmsg((*C.sqlite3)(c.sqlite3)))

	if message == "" {
		message = C.GoString(C.sqlite3_errstr(C.int(code)))
	}

	return fmt.Errorf("SQLite3 Error[%d]: %s", code, message)
}

// Execute a query
func (c *Connection) Exec(ctx context.Context, query []byte, params ...StatementParameter) (*Result, error) {
	var stmt *Statement
	var err error
	var errCode int

	if stmt, errCode, err = c.Prepare(ctx, query); errCode != 0 {
		if errCode == SQLITE_AUTH {
			return nil, c.authorizationError
		}

		return nil, err
	}

	defer func() {
		err := stmt.Finalize()

		if err != nil {
			log.Fatalln("Error finalizing statement:", err)
		}
	}()

	result := NewResult()

	err = stmt.Exec(result, params...)

	return result, err
}

// Interrupt all queries for connection
func (c *Connection) Interrupt() {
	if c.sqlite3 == nil {
		return
	}

	C.sqlite3_interrupt((*C.sqlite3)(c.sqlite3))
}

// Get last insert id
func (c *Connection) LastInsertRowID() int64 {
	return int64(C.sqlite3_last_insert_rowid((*C.sqlite3)(c.sqlite3)))
}

// Prepare query
func (c *Connection) Prepare(ctx context.Context, query []byte) (*Statement, int, error) {
	return NewStatement(ctx, c, query)
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

	r := int(C.sqlite3_db_readonly((*C.sqlite3)(c.sqlite3), cSchema))

	if r == -1 {
		return false
	} else {
		return r != 0
	}
}

func (c *Connection) Rollback() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_ROLLBACK, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (c *Connection) SetAuthorizationError(err error) {
	c.authorizationError = err
}

// Set last insert id
func (c *Connection) SetLastInsertId(v int64) {
	C.sqlite3_set_last_insert_rowid((*C.sqlite3)(c.sqlite3), C.sqlite3_int64(v))
}

// Register a Go function as an authorizer callback function.
// https://www.sqlite.org/c3ref/set_authorizer.html
func (c *Connection) Authorizer(authorizer Authorizer) {
	c.authorizer = authorizer

	connectionHandle := cgo.NewHandle(c)

	C.sqlite3_set_authorizer(
		(*C.sqlite3)(c.sqlite3),
		(*[0]byte)(C.go_authorizer),
		unsafe.Pointer(connectionHandle),
	)
}

//export go_authorizer
func go_authorizer(connectionHandle C.uintptr_t, action C.int, arg3, arg4, arg5, arg6 *C.char) C.int {
	handle := cgo.Handle(connectionHandle)

	c := handle.Value().(*Connection)

	if c.authorizer != nil {
		return C.int(
			c.authorizer(
				int(action),
				C.GoString(arg3),
				C.GoString(arg4),
				C.GoString(arg5),
				C.GoString(arg6),
			),
		)
	}

	return C.int(0)
}

// Vacuum the database to remove unused pages and repack the database file.
//
// This operation should be done when the database is not in use as it locks the
// database EXCLUSIVELY. The connection manager should drain any active connections
// and ensure any changes are checkpointed before calling this method. Otherwise,
// the Checkpointer may reference pages that no longer exist in the database file.
func (c *Connection) Vacuum() error {
	rc := C.sqlite3_exec(c.sqlite3, SQL_VACUUM, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}
