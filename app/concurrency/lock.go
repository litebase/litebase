package concurrency

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

var ActiveLock *os.File

func Lock() bool {
	// openLockStart := time.Now()
	file, err := os.OpenFile(lockPath(), os.O_RDWR|os.O_CREATE, 0666)
	// fmt.Println("Open lock after: ", time.Since(openLockStart))

	if err != nil {
		return false
	}

	err = retryLock(10*time.Microsecond, func() error {
		return syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	})

	if err != nil {
		return false
	}

	ActiveLock = file

	return true
}

func Unlock() {
	// unlockStart := time.Now()

	if ActiveLock == nil {
		return
	}

	syscall.Flock(int(ActiveLock.Fd()), syscall.LOCK_UN|syscall.LOCK_NB)
	// Set the active lock to nil after the file is unlocked
	ActiveLock = nil
	// fmt.Println("Unlocked file: ", time.Since(unlockStart))
}

func lockPath() string {
	// TODO: Update to actual database path
	return fmt.Sprintf("%s/%s.lock",
		os.Getenv("DATABASE_DIRECTORY"), os.Getenv("DATABASE_NAME"),
	)
}

func retryLock(sleep time.Duration, f func() error) (err error) {
	start := time.Now()
	timeout := start.Add(3 * time.Second)
	i := 0

	for time.Now().Before(timeout) {
		err = f()

		if err == nil {
			return nil
		}

		time.Sleep(sleep)

		i++
	}

	return fmt.Errorf("timed out after %s, last error: %s", time.Since(start), err)
}
