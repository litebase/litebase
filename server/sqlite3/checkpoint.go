package sqlite3

/*
#include"./sqlite3.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
)

type CheckpointResult struct {
	WalLogSize            int
	NumFramesCheckpointed int
	Result                int
}

func Checkpoint(db *C.sqlite3, checkpointHook func(CheckpointResult)) (CheckpointResult, error) {
	var pWalLogSize, pNumFramesCheckpointed C.int

	res := C.sqlite3_wal_checkpoint_v2(
		db,
		nil, // The name of the database to checkpoint. If NULL, then it will checkpoint all attached databases.
		C.SQLITE_CHECKPOINT_TRUNCATE,
		&pWalLogSize,
		&pNumFramesCheckpointed,
	)

	if res != C.SQLITE_OK {
		return CheckpointResult{}, errors.New(C.GoString(C.sqlite3_errstr(res)))
	}

	result := CheckpointResult{
		WalLogSize:            int(pWalLogSize),
		NumFramesCheckpointed: int(pNumFramesCheckpointed),
		Result:                int(res),
	}

	checkpointHook(result)

	return result, nil
}
