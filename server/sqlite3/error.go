package sqlite3

/*
#include "./sqlite3.h"
*/
import "C"
import (
	"fmt"
)

func (c *Connection) Error(code C.int) error {
	message := C.GoString(C.sqlite3_errmsg((*C.sqlite3)(c)))

	if message == "" {
		message = C.GoString(C.sqlite3_errstr(C.int(code)))
	}

	return fmt.Errorf("SQLite3 Error[%d]: %s", code, message)
}
