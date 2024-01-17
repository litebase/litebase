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
	"log"
	"unsafe"
)

var storage StorageDriver

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

	fileName := C.GoString(name)

	log.Println("OPEN", fileName)

	return sqliteOK
}

//export goXRead
func goXRead(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	// log.Println("READ")

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]
	n, err := storage.ReadAt(goBuffer, int64(iOfst))

	if err == io.EOF {
		// return C.SQLITE_IOERR_SHORT_READ
		if n < len(goBuffer) {
			for i := n; i < len(goBuffer); i++ {
				goBuffer[i] = 0
			}
		}
	} else if err != nil {
		return errToC(err)
	}

	return sqliteOK
}

//export goXWrite
func goXWrite(pFile *C.sqlite3_file, zBuf unsafe.Pointer, iAmt C.int, iOfst C.sqlite3_int64) C.int {
	// log.Println("WRITE")

	goBuffer := (*[1 << 28]byte)(zBuf)[:int(iAmt):int(iAmt)]
	_, err := storage.WriteAt(goBuffer, int64(iOfst))

	// _, err := storage.WriteAt(C.GoBytes(unsafe.Pointer(zBuf), iAmt), int64(iOfst))

	if err != nil {
		log.Println("WRITE ERROR", err)
		return C.SQLITE_IOERR_WRITE
	}

	return sqliteOK
}

//export goXFileSize
func goXFileSize(pFile *C.sqlite3_file, pSize *C.sqlite3_int64) C.int {
	// log.Println("SIZE")

	size, err := storage.Size()

	if err != nil {
		log.Println("SIZE ERROR", err)
		return C.SQLITE_IOERR_FSTAT
	}

	*pSize = C.sqlite3_int64(size)

	return sqliteOK
}

func errToC(err error) C.int {
	if e, ok := err.(sqliteError); ok {
		return C.int(e.code)
	}
	return C.int(GenericError.code)
}
