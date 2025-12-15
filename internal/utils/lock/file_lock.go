//go:build !windows

package lock

import (
	"os"
	"syscall"
)

// LockFile acquires an exclusive lock on the given file.
// Returns true if the lock was acquired, false if the file is already locked.
func LockFile(file *os.File) (bool, error) {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	if err != nil {
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return false, nil // File is already locked
		}

		return false, err
	}

	return true, nil
}

// UnlockFile releases the lock on the given file.
func UnlockFile(file *os.File) error {
	return syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
}
