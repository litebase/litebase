package sqlite3

/*
#include "./sqlite3.h"
#include <stdlib.h>

extern int go_changeset_filter(void *pCtx, char *zTab);
extern int go_changeset_conflict(void *pCtx, int eConflict, sqlite3_changeset_iter *p);
*/
import "C"

import (
	"log"
	"unsafe"

	"github.com/google/uuid"
)

type Session struct {
	id             string
	sqlite3Session *C.sqlite3_session
}

func CreateSession(connection *C.sqlite3) *Session {
	var session *C.sqlite3_session

	// Create the session
	rc := C.sqlite3session_create((*C.sqlite3)(connection), C.CString("main"), &session)

	if rc != SQLITE_OK {
		log.Fatalln("Session Create Error:", rc)
		return nil
	}

	// Attach the session to all tables of the database
	rc = C.sqlite3session_attach(session, nil)

	if rc != SQLITE_OK {
		log.Fatalln("Session Attach Error:", rc)
		return nil
	}

	return &Session{
		id:             uuid.New().String(),
		sqlite3Session: session,
	}
}

func (s *Session) Delete() {
	C.sqlite3session_delete(s.sqlite3Session)
}

func (s *Session) ChangeSet() *ChangeSet {
	var changeSetLength C.int
	// The unsafe pointer to the changeset
	var changeSet unsafe.Pointer

	// Get the changeset
	rc := C.sqlite3session_changeset(s.sqlite3Session, &changeSetLength, &changeSet)

	if rc != SQLITE_OK {
		log.Fatalln("Session Changeset Error:", rc, C.GoString(C.sqlite3_errstr(rc)))
		return nil
	}

	// Convert the changeset to a byte array
	changeSetBytes := C.GoBytes(changeSet, changeSetLength)

	// Free the changeset
	C.sqlite3_free(changeSet)

	s.Delete()

	return &ChangeSet{
		data: changeSetBytes,
	}
}

func ApplyChangeSet(connection *C.sqlite3, changeSet []byte) {
	var changeSetLength C.int
	var changeSetPointer unsafe.Pointer

	// Convert the changeset to a byte array
	changeSetLength = C.int(len(changeSet))
	// Convert the changest to a C pointer
	changeSetPointer = C.CBytes(changeSet)

	// Apply the changeset
	C.sqlite3changeset_apply(
		connection,
		changeSetLength,
		changeSetPointer,
		(*[0]byte)(C.go_changeset_filter),
		(*[0]byte)(C.go_changeset_conflict),
		nil,
	)
}

//export go_changeset_filter
func go_changeset_filter(pCtx unsafe.Pointer, zTab *C.char) C.int {
	return C.int(1)
}

//export go_changeset_conflict
func go_changeset_conflict(pCtx unsafe.Pointer, eConflict C.int, p *C.sqlite3_changeset_iter) C.int {
	// If the conflict is associated with a contraint violation, then omit the changeset.
	// Contraints such as UNIQUE, NOT NULL (?), CHECK, and FOREIGN KEY are not supported.
	if eConflict == SQLITE_CHANGESET_CONSTRAINT {
		return C.int(SQLITE_CHANGESET_OMIT)
	}

	// If the conflict is associated with a foreign key violation, then omit the changeset.
	if eConflict == SQLITE_CHANGESET_FOREIGN_KEY {
		return C.int(SQLITE_CHANGESET_OMIT)
	}

	// If the conflict is associated with a missing row, then omit the changeset.
	if eConflict == SQLITE_CHANGESET_NOTFOUND {
		return C.int(SQLITE_CHANGESET_OMIT)
	}

	// If the conflict is associated with a data change, then replace the data.
	if eConflict == SQLITE_CHANGESET_DATA {
		return C.int(SQLITE_CHANGESET_REPLACE)
	}

	// If the conflict processing an INSERT has a conflict with the PRIMARY KEY, then abort the changeset.
	if eConflict == SQLITE_CHANGESET_CONFLICT {
		return C.int(SQLITE_CHANGESET_OMIT)
	}

	return C.int(SQLITE_CHANGESET_ABORT)
}
