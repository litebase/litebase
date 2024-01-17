package sqlite3

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"unsafe"
)

/*
#include "./sqlite3.h"
#include <stdlib.h>
*/
import "C"

type ChangeSet struct {
	data    []byte
	changes []string
}

// Read the SQLite3 change set and gather the pages and primary keys
func (c *ChangeSet) ReadHashes() (hashes []string, err error) {
	var changesetIterator *C.sqlite3_changeset_iter

	// Create an iterator to iterate through the changeset
	rc := C.sqlite3changeset_start(&changesetIterator, C.int(len(c.data)), C.CBytes(c.data))

	if rc != C.SQLITE_OK {
		return hashes, errors.New(C.GoString(C.sqlite3_errstr(rc)))
	}

	for {
		// Get the next change
		rc := C.sqlite3changeset_next(changesetIterator)

		if rc != C.SQLITE_ROW {
			break
		}

		// Get the table name
		var tableName *C.char
		var pkColumns *C.uchar
		columnCount := C.int(0)
		operation := C.int(0)
		indirect := C.int(0)
		defer C.free(unsafe.Pointer(tableName))

		rc = C.sqlite3changeset_pk(changesetIterator, &pkColumns, &columnCount)

		if rc != C.SQLITE_OK {
			return hashes, errors.New(C.GoString(C.sqlite3_errstr(rc)))
		}

		// primaryKeyColumns in C is a pointer to an array of unsigned chars
		// We need to convert this to a Go byte array
		primaryKeyColumns := C.GoBytes(unsafe.Pointer(pkColumns), columnCount)

		// Get the operation
		rc = C.sqlite3changeset_op(changesetIterator, &tableName, &columnCount, &operation, &indirect)

		if rc != C.SQLITE_OK {
			return hashes, errors.New(C.GoString(C.sqlite3_errstr(rc)))
		}

		table := C.GoString(tableName)
		// var operationType string

		// switch int(operation) {
		// case SQLITE_INSERT:
		// 	operationType = "insert"
		// case SQLITE_UPDATE:
		// 	operationType = "update"
		// case SQLITE_DELETE:
		// 	operationType = "delete"
		// }

		var columnValues []string

		// iterate through the columns
		for i := C.int(0); i < columnCount; i++ {
			var columnName *C.char
			defer C.free(unsafe.Pointer(columnName))
			var pVal *C.sqlite3_value

			if operation == SQLITE_DELETE {
				rc = C.sqlite3changeset_old(changesetIterator, i, &pVal)
			} else {
				rc = C.sqlite3changeset_new(changesetIterator, i, &pVal)
			}

			if rc != C.SQLITE_OK {
				return hashes, errors.New(C.GoString(C.sqlite3_errstr(rc)))
			}

			columnValues = append(columnValues, C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_value_text(pVal)))))
		}

		var primaryKey []string

		for i, columnValue := range columnValues {
			if primaryKeyColumns[i] == 1 {
				primaryKey = append(primaryKey, columnValue)
			}
		}

		hash := sha1.Sum([]byte(fmt.Sprintf("%s:%v", table, primaryKey)))

		c.changes = append(c.changes, fmt.Sprintf("%x", hash))
	}

	rc = C.sqlite3changeset_finalize(changesetIterator)

	if rc != C.SQLITE_OK {
		return hashes, errors.New(C.GoString(C.sqlite3_errstr(rc)))
	}

	hashes = c.changes

	return hashes, nil
}
