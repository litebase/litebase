package sqlite3

import (
	"context"
	"errors"
	"log"
	"runtime/cgo"
	"time"
	"unsafe"
)

/*
#include "./sqlite3.h"
#include <stdlib.h>

extern int go_authorizer(void* pArg, int actionCode, char* arg1, char* arg2, char* arg3, char* arg4);
extern void go_commit_hook(uintptr_t connectionHandle);
extern int go_progress_handler(uintptr_t connectionHandle);
*/
import "C"

var (
	SQL_BEGIN           = (*C.char)(unsafe.Pointer(&[]byte("BEGIN")[0]))
	SQL_BEGIN_DEFERRED  = (*C.char)(unsafe.Pointer(&[]byte("BEGIN DEFERRED")[0]))
	SQL_BEGIN_IMMEDIATE = (*C.char)(unsafe.Pointer(&[]byte("BEGIN IMMEDIATE")[0]))
)

type OpenFlags C.int
type Connection struct {
	authorizer Authorizer
	committing bool
	committed  chan struct{}
	context    context.Context
	sqlite3    *C.sqlite3
}

type Authorizer func(action int, arg1, arg2, dbName, triggerOrView string) (allow int)

// TODO: These singletons need to be attached to the connection instead of being global
var authorizerCallback Authorizer

func init() {
	if err := C.sqlite3_initialize(); err != SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errstr(err))))
	}
}

func (c *Connection) Base() *C.sqlite3 {
	return (*C.sqlite3)(c.sqlite3)
}

func Open(ctx context.Context, path, vfsId string, flags OpenFlags) (*Connection, error) {
	var cVfs, cName *C.char
	c := &Connection{
		committing: false,
		committed:  make(chan struct{}, 1),
		context:    ctx,
	}

	// Set vfs
	cVfs = C.CString(vfsId)
	defer C.free(unsafe.Pointer(cVfs))

	cName = C.CString(path)
	defer C.free(unsafe.Pointer(cName))

	var vfs *C.char
	if len(vfsId) == 0 {
		vfs = nil
	} else {
		vfs = cVfs
	}

	// Call sqlite3_open_v2
	if err := C.sqlite3_open_v2(cName, &c.sqlite3, C.int(flags), vfs); err != SQLITE_OK {
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

	// TODO: See if we can make use of these, probably for change detection
	// c.commitHook()
	// c.progressCallback()

	return c, nil
}

func (c *Connection) Begin() error {
	rc := C.sqlite3_exec((*C.sqlite3)(c.sqlite3), SQL_BEGIN, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (c *Connection) BeginDeferred() error {
	rc := C.sqlite3_exec((*C.sqlite3)(c.sqlite3), SQL_BEGIN_DEFERRED, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (c *Connection) BeginImmediate() error {
	rc := C.sqlite3_exec((*C.sqlite3)(c.sqlite3), SQL_BEGIN_IMMEDIATE, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
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

// Close Connection
func (c *Connection) Close() error {
	var result error

	// Close database connection
	if err := C.sqlite3_close_v2((*C.sqlite3)(c.sqlite3)); err != SQLITE_OK {
		result = errors.New(C.GoString(C.sqlite3_errstr(err)))
	}

	// Return any errors
	return result
}

func (c *Connection) Commit() error {
	sql := (*C.char)(unsafe.Pointer(&[]byte("COMMIT")[0]))

	rc := C.sqlite3_exec((*C.sqlite3)(c.sqlite3), sql, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

// TODO: Need to handle rollback event as well
func (c *Connection) Committing(f func() error) error {
	c.committing = true

	if err := f(); err != nil {
		c.committing = false
		return err
	}

	c.committing = false

	return nil
}

// Register a Go function as a commit hook on the SQLite database connection.
// The hook function will be called whenever a transaction is committed.
// The hook function should return true to rollback the transaction or false to commit the transaction.
func (c *Connection) commitHook() {
	connectionHandle := cgo.NewHandle(c)

	C.sqlite3_commit_hook(
		(*C.sqlite3)(c.sqlite3),
		(*[0]byte)(C.go_commit_hook),
		unsafe.Pointer(connectionHandle),
	)
}

//export go_commit_hook
func go_commit_hook(connectionHandle C.uintptr_t) {
	handle := cgo.Handle(connectionHandle)
	// defer handle.Delete()
	c := handle.Value().(*Connection)

	if !c.committing {
		return
	}

	// log.Println("Commit hook called")
	// c.committed <- struct{}{}
	// log.Println("Commit hook done")
}

// Execute a query
func (c *Connection) Exec(ctx context.Context, query string, params ...StatementParameter) (Result, error) {
	var stmt *Statement
	var err error

	if stmt, err = c.Prepare(ctx, query); err != nil {
		return Result{}, err
	}

	defer func() {
		err := stmt.Finalize()

		if err != nil {
			log.Fatalln("Error finalizing statement:", err)
		}
	}()

	return stmt.Exec(params...)
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

// Use sqlite3_file_control() to set the SQLITE_FCNTL_PERSIST_WAL file control to
// true to enable the persistent WAL mode. This will cause the WAL file to be
// persisted to disk after the last connection to the database is closed.
func (c *Connection) PersistWal() error {
	var persist C.int = 1

	rc := C.sqlite3_file_control((*C.sqlite3)(c.sqlite3), nil, C.SQLITE_FCNTL_PERSIST_WAL, unsafe.Pointer(&persist))

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

func (c *Connection) progressCallback() {
	connectionHandle := cgo.NewHandle(c)

	C.sqlite3_progress_handler(
		(*C.sqlite3)(c.sqlite3),
		1,
		(*[0]byte)(C.go_progress_handler),
		unsafe.Pointer(connectionHandle),
	)
}

//export go_progress_handler
func go_progress_handler(connectionHandle C.uintptr_t) C.int {
	handle := cgo.Handle(connectionHandle)
	defer handle.Delete()
	c := handle.Value().(*Connection)

	// log.Println("Progress handler called")
	if !c.committing {
		return 0
	}

	// runtime.Gosched()

	return SQLITE_OK
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
	sql := (*C.char)(unsafe.Pointer(&[]byte("ROLLBACK")[0]))

	rc := C.sqlite3_exec((*C.sqlite3)(c.sqlite3), sql, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}

// Set last insert id
func (c *Connection) SetLastInsertId(v int64) {
	C.sqlite3_set_last_insert_rowid((*C.sqlite3)(c.sqlite3), C.sqlite3_int64(v))
}

// Register a Go function as an authorizer callback function.
// https://www.sqlite.org/c3ref/set_authorizer.html
func (c *Connection) Authorizer(authorizer Authorizer) {
	authorizerCallback = authorizer

	// TODO: We need to ensure that our authorizer of the struct is called by cgo
	C.sqlite3_set_authorizer((*C.sqlite3)(c.sqlite3), (*[0]byte)(C.go_authorizer), nil)
}

//export go_authorizer
func go_authorizer(userInfo unsafe.Pointer, action C.int, arg1, arg2, dbName, triggerOrView *C.char) C.int {
	// if authorizerCallback != nil {
	// 	return C.int(authorizerCallback(int(action), C.GoString(arg1), C.GoString(arg2), C.GoString(dbName), C.GoString(triggerOrView)))
	// }

	return C.int(0)
}

// Vacuum the database to remove unused pages and repack the database file.
//
// This operation should be done when the database is not in use as it locks the
// database EXCLUSIVELY. The connection manager should drain any active connections
// and ensure any changes are checkpointed before calling this method. Otherwise,
// the Checkpointer may reference pages that no longer exist in the database file.
func (c *Connection) Vacuum() error {
	sql := (*C.char)(unsafe.Pointer(&[]byte("VACUUM")[0]))

	rc := C.sqlite3_exec((*C.sqlite3)(c.sqlite3), sql, nil, nil, nil)

	if rc != SQLITE_OK {
		return errors.New(C.GoString(C.sqlite3_errstr(C.int(rc))))
	}

	return nil
}
