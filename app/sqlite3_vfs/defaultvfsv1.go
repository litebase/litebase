package sqlite3_vfs

import (
	"crypto/rand"
	"log"
	"time"
)

type defaultVFSv1 struct {
	VFS
}

func (v *defaultVFSv1) Randomness(n []byte) int {
	i, err := rand.Read(n)
	if err != nil {
		log.Fatal(err)
	}
	return i
}

func (v *defaultVFSv1) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (v *defaultVFSv1) CurrentTime() time.Time {
	return time.Now()
}
