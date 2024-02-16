package vfs

/*
#cgo linux LDFLAGS: -Wl,--unresolved-symbols=ignore-in-object-files
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <litebasedb_vfs.h>
*/
import "C"

import (
	"io"
	"time"
	"unsafe"
)

var storage StorageDriver
var fileLockCount int64

func RegisterVFS(stroageDriver StorageDriver) error {
	storage = stroageDriver

	rc := C.newVfs()

	if rc == C.SQLITE_OK {
		return nil
	}

	return errFromCode(int(rc))
}

//export goXOpen
func goXOpen(vfs *C.sqlite3_vfs, name *C.char, file *C.sqlite3_file, flags C.int, outFlags *C.int) C.int {
	return sqliteOK
}

//export goXDelete
func goXDelete(vfs *C.sqlite3_vfs, name *C.char, syncDir C.int) C.int {
	return sqliteOK
}

//export goXAccess
func goXAccess(vfs *C.sqlite3_vfs, name *C.char, flags C.int, resOut *C.int) C.int {
	*resOut = C.int(1)

	return sqliteOK
}

//export goXSleep
func goXSleep(cvfs *C.sqlite3_vfs, microseconds C.int) C.int {
	d := time.Duration(microseconds) * time.Microsecond

	time.Sleep(d)

	return sqliteOK
}

//export goXClose
func goXClose(pFile *C.sqlite3_file) C.int {
	return sqliteOK
}

//export goXRead
func goXRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]
	data, err := storage.ReadAt(int64(iOfst))
	n := copy(goBuffer, data)

	if n < len(goBuffer) && err == io.EOF {
		for i := n; i < len(goBuffer); i++ {
			goBuffer[i] = 0
		}

		return errToC(IOErrorShortRead)
	}

	if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goXWrite
func goXWrite(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]
	_, err := storage.WriteAt(goBuffer, int64(iOfst))

	if err != nil {
		return C.SQLITE_IOERR_WRITE
	}

	return sqliteOK
}

//export goXFileSize
func goXFileSize(pFile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	size, err := storage.Size()

	if err != nil {
		return C.SQLITE_IOERR_FSTAT
	}

	*pSize = C.sqlite3_int64(size)

	return sqliteOK
}

//export goXLock
func goXLock(pFile *C.sqlite3_file, lockType C.int) C.int {
	return sqliteOK
}

//export goXUnlock
func goXUnlock(pFile *C.sqlite3_file, lockType C.int) C.int {
	return sqliteOK
}

//export goXCheckReservedLock
func goXCheckReservedLock(pFile *C.sqlite3_file, pResOut *C.int) C.int {
	return sqliteOK
}

func errToC(err error) C.int {
	if e, ok := err.(sqliteError); ok {
		return C.int(e.code)
	}
	return C.int(GenericError.code)
}
