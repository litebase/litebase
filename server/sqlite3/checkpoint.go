package sqlite3

/*
#include"./sqlite3.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

type CheckpointResult struct {
	WalLogSize            int
	NumFramesCheckpointed int
	Result                int
}

func Checkpoint(db *C.sqlite3) (CheckpointResult, error) {
	var pWalLogSize, pNumFramesCheckpointed C.int
	var cName *C.char

	cName = C.CString("main")
	defer C.free(unsafe.Pointer(cName))

	res := C.sqlite3_wal_checkpoint_v2(
		db,
		cName, // The name of the database to checkpoint. If NULL, then it will checkpoint all attached databases.
		C.SQLITE_CHECKPOINT_TRUNCATE,
		&pWalLogSize,
		&pNumFramesCheckpointed,
	)

	if res != C.SQLITE_OK {
		return CheckpointResult{}, errors.New(C.GoString(C.sqlite3_errstr(res)))
	}

	return CheckpointResult{
		WalLogSize:            int(pWalLogSize),
		NumFramesCheckpointed: int(pNumFramesCheckpointed),
		Result:                int(res),
	}, nil
}
