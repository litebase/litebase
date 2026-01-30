package sqlite3

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"runtime/cgo"
	"time"
	"unsafe"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/constants"
)

/*
#include "./sqlite3.h"
#include <stdlib.h>
#include <stdint.h>

extern int go_authorizer(uintptr_t connectionHandle, int actionCode, char* arg1, char* arg2, char* arg3, char* arg4);
extern int go_progress_handler(uintptr_t connectionHandle);

// Forward declaration for vector extension initialization
extern int sqlite3_vectorextension_init(sqlite3 *db, char **pzErrMsg, const void *pApi);

// Forward declaration for vector_scan registration
typedef struct {
	char *vfsID;
	char *databaseID;
	char *branchID;
} VectorScanContext;

// Forward declaration for vector_index registration
typedef struct {
	char *vfsID;
	char *databaseID;
	char *branchID;
} VectorIndexContext;

extern int sqlite3_register_vector_scan(sqlite3 *db, void *ctx, char *vfsID, char *databaseID, char *branchID);
extern int sqlite3_register_vector_index_with_context(sqlite3 *db, void *ctx, char *vfsID, char *databaseID, char *branchID);
extern int sizeof_VectorScanContext;
extern int sizeof_VectorIndexContext;
*/
import "C"

var (
	SQL_BEGIN           = (*C.char)(utils.StaticSafeCString("BEGIN"))
	SQL_BEGIN_DEFERRED  = (*C.char)(utils.StaticSafeCString("BEGIN DEFERRED"))
	SQL_BEGIN_EXCLUSIVE = (*C.char)(utils.StaticSafeCString("BEGIN EXCLUSIVE"))
	SQL_BEGIN_IMMEDIATE = (*C.char)(utils.StaticSafeCString("BEGIN IMMEDIATE"))
	SQL_COMMIT          = (*C.char)(utils.StaticSafeCString("COMMIT"))
	SQL_ROLLBACK        = (*C.char)(utils.StaticSafeCString("ROLLBACK"))
	SQL_VACUUM          = (*C.char)(utils.StaticSafeCString("VACUUM"))
)

type OpenFlags C.int
type Connection struct {
	authorizer         Authorizer
	authorizerHandle   cgo.Handle
	authorizationError error
	cName              *C.char
	context            context.Context
	cVfs               *C.char
	id                 string
	sqlite3            *C.sqlite3
}

type Authorizer func(action int, arg1, arg2, dbName, triggerOrView string) (allow int32)

func init() {
	if err := C.sqlite3_initialize(); err != SQLITE_OK {
		panic(errors.New(C.GoString(C.sqlite3_errstr(err))))
	}
}

func (c *Connection) Base() *C.sqlite3 {
	return (*C.sqlite3)(c.sqlite3)
}

func Open(ctx context.Context, path, vfsId string, flags OpenFlags) (*Connection, error) {
	// Validate input parameters to prevent potential issues with C.CString
	if path == "" {
		return nil, errors.New("path cannot be empty")
	}

	cName, err := utils.SafeCString(path)
	if err != nil {
		return nil, fmt.Errorf("failed to convert path to C string: %v", err)
	}

	cVfs, err := utils.SafeCString(vfsId)
	if err != nil {
		C.free(unsafe.Pointer(cName))
		return nil, fmt.Errorf("failed to convert vfsId to C string: %v", err)
	}

	c := &Connection{
		cName:   (*C.char)(cName),
		context: ctx,
		cVfs:    (*C.char)(cVfs),
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

	// Register vector extension
	if err := c.registerVectorExtension(); err != nil {
		C.sqlite3_close_v2(c.sqlite3)

		return nil, fmt.Errorf("failed to register vector extension: %w", err)
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
	durationInt32, err := utils.SafeInt64ToInt32(int64(duration / time.Millisecond))

	if err != nil {
		return err
	}

	if err := C.sqlite3_busy_timeout((*C.sqlite3)(c.sqlite3), C.int(durationInt32)); err != SQLITE_OK {
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

	// Clean up authorizer handle before closing
	if c.authorizerHandle != 0 {
		c.authorizerHandle.Delete()
		c.authorizerHandle = 0
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
		var int32Code int32
		var err error

		int32Code, err = utils.SafeIntToInt32(code)

		if err != nil {
			slog.Error("Failed to convert error code to int32", "error", err)
		}

		message = C.GoString(C.sqlite3_errstr(C.int(int32Code)))
	}

	return fmt.Errorf("SQLite3 Error[%d]: %s", code, message)
}

// Execute a query
func (c *Connection) Exec(ctx context.Context, query string, params ...StatementParameter) (*Result, error) {
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
func (c *Connection) Prepare(ctx context.Context, query string) (*Statement, int, error) {
	return NewStatement(ctx, c, query)
}

// Get Read-only state. Also returns false if database not found
func (c *Connection) Readonly(schema string) bool {
	var cSchema *C.char

	// Set schema to default if empty string
	if schema == "" {
		schema = "main"
	}

	schemaString, err := utils.SafeCString(schema)
	if err != nil {
		return false
	}
	cSchema = (*C.char)(schemaString)
	defer C.free(unsafe.Pointer(schemaString))

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
//
//go:nocheckptr
func (c *Connection) Authorizer(authorizer Authorizer) {
	// Release any existing handle to prevent leaks
	if c.authorizerHandle != 0 {
		c.authorizerHandle.Delete()
	}

	c.authorizer = authorizer

	if authorizer != nil {
		c.authorizerHandle = cgo.NewHandle(c)

		//nolint:govet
		C.sqlite3_set_authorizer(
			(*C.sqlite3)(c.sqlite3),
			(*[0]byte)(C.go_authorizer),
			unsafe.Pointer(uintptr(c.authorizerHandle)),
		)
	} else {
		// Clear the authorizer if nil is passed
		c.authorizerHandle = 0
		C.sqlite3_set_authorizer(
			(*C.sqlite3)(c.sqlite3),
			nil,
			nil,
		)
	}
}

// registerVectorExtension initializes the vector extension for this connection
func (c *Connection) registerVectorExtension() error {
	var errMsg *C.char

	rc := C.sqlite3_vectorextension_init(c.sqlite3, &errMsg, nil)

	if rc != SQLITE_OK {
		defer C.sqlite3_free(unsafe.Pointer(errMsg))

		return fmt.Errorf("vector extension initialization failed: %s", C.GoString(errMsg))
	}

	return nil
}

// RegisterVectorIndexFunction registers the vector_index virtual table with connection context
func (c *Connection) RegisterVectorIndexFunction(vfsID, databaseID, branchID string) error {
	// Create context structure
	ctxSize := C.sizeof_VectorIndexContext
	ctxPtr := C.sqlite3_malloc(C.int(ctxSize))

	if ctxPtr == nil {
		return fmt.Errorf("failed to allocate vector_index context")
	}

	// Convert strings to C strings (will be freed when module is destroyed)
	cVfsID, err := utils.SafeCString(vfsID)
	if err != nil {
		C.sqlite3_free(ctxPtr)
		return err
	}

	cDatabaseID, err := utils.SafeCString(databaseID)
	if err != nil {
		C.free(unsafe.Pointer(cVfsID))
		C.sqlite3_free(ctxPtr)
		return err
	}

	cBranchID, err := utils.SafeCString(branchID)
	if err != nil {
		C.free(unsafe.Pointer(cVfsID))
		C.free(unsafe.Pointer(cDatabaseID))
		C.sqlite3_free(ctxPtr)
		return err
	}

	// Call C function to register with context
	rc := C.sqlite3_register_vector_index_with_context(
		c.sqlite3,
		ctxPtr,
		(*C.char)(cVfsID),
		(*C.char)(cDatabaseID),
		(*C.char)(cBranchID))

	if rc != SQLITE_OK {
		C.free(unsafe.Pointer(cVfsID))
		C.free(unsafe.Pointer(cDatabaseID))
		C.free(unsafe.Pointer(cBranchID))
		C.sqlite3_free(ctxPtr)
		return fmt.Errorf("failed to register vector_index module: %d", rc)
	}

	return nil
}

// RegisterVectorScanFunction registers the vector_scan function with connection context
func (c *Connection) RegisterVectorScanFunction(vfsID, databaseID, branchID string) error {
	// Create context structure
	ctxSize := C.sizeof_VectorScanContext
	ctxPtr := C.sqlite3_malloc(C.int(ctxSize))

	if ctxPtr == nil {
		return fmt.Errorf("failed to allocate vector_scan context")
	}

	// Convert strings to C strings (will be freed when function is destroyed)
	cVfsID, err := utils.SafeCString(vfsID)
	if err != nil {
		C.sqlite3_free(ctxPtr)
		return err
	}

	cDatabaseID, err := utils.SafeCString(databaseID)
	if err != nil {
		C.free(unsafe.Pointer(cVfsID))
		C.sqlite3_free(ctxPtr)
		return err
	}

	cBranchID, err := utils.SafeCString(branchID)
	if err != nil {
		C.free(unsafe.Pointer(cVfsID))
		C.free(unsafe.Pointer(cDatabaseID))
		C.sqlite3_free(ctxPtr)
		return err
	}

	// Call C function to register with context
	rc := C.sqlite3_register_vector_scan(
		c.sqlite3,
		ctxPtr,
		(*C.char)(cVfsID),
		(*C.char)(cDatabaseID),
		(*C.char)(cBranchID))

	if rc != SQLITE_OK {
		C.free(unsafe.Pointer(cVfsID))
		C.free(unsafe.Pointer(cDatabaseID))
		C.free(unsafe.Pointer(cBranchID))
		C.sqlite3_free(ctxPtr)
		return fmt.Errorf("failed to register vector_scan function: %d", rc)
	}

	return nil
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
