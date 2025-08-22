//go:build windows

package lock

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	LOCKFILE_EXCLUSIVE_LOCK   = 0x00000002
	LOCKFILE_FAIL_IMMEDIATELY = 0x00000001
	ERROR_LOCK_VIOLATION      = 33
)

// LockFile acquires an exclusive lock on the given file.
// Returns true if the lock was acquired, false if the file is already locked.
func LockFile(file *os.File) (bool, error) {
	var overlapped syscall.Overlapped

	ret, _, err := procLockFileEx.Call(
		uintptr(file.Fd()),
		uintptr(LOCKFILE_EXCLUSIVE_LOCK|LOCKFILE_FAIL_IMMEDIATELY),
		0,
		0,
		0xFFFFFFFF, // Lock entire file
		uintptr(unsafe.Pointer(&overlapped)),
	)

	if ret == 0 {
		if errno, ok := err.(syscall.Errno); ok {
			if errno == ERROR_LOCK_VIOLATION {
				return false, nil // File is already locked
			}
		}
		return false, err
	}

	return true, nil
}

// UnlockFile releases the lock on the given file.
func UnlockFile(file *os.File) error {
	var overlapped syscall.Overlapped

	ret, _, err := procUnlockFileEx.Call(
		uintptr(file.Fd()),
		0,
		0,
		0xFFFFFFFF, // Unlock entire file
		uintptr(unsafe.Pointer(&overlapped)),
	)

	if ret == 0 {
		return err
	}

	return nil
}
