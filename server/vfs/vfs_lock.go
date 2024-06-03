package vfs

/*
#include <vfs.h>
*/
import "C"
import (
	"sync"
)

type VfsLock struct {
	connections map[string]int
	mutex       *sync.RWMutex
}

func NewVfsLock() *VfsLock {
	return &VfsLock{
		connections: make(map[string]int),
		mutex:       &sync.RWMutex{},
	}
}

func (l *VfsLock) CheckReservedLock() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	for _, v := range l.connections {
		if v >= C.SQLITE_LOCK_RESERVED {
			return true
		}
	}

	return false
}

func (l *VfsLock) Lock(connectionId string, lockType int) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// // Check that there are no other connections with a lock acquired. This
	// // connection should also have a pending lock.
	// if lockType == C.SQLITE_LOCK_EXCLUSIVE {
	// 	for k, v := range l.connections {
	// 		if v > C.SQLITE_LOCK_NONE && k != connectionId {
	// 			// log.Println("Connection has a lock acquired")
	// 			return false
	// 		}
	// 	}
	// }

	// // A shared lock can be acquired as long as there are no other connections
	// // with exclusive or pending locks.
	// if lockType == C.SQLITE_LOCK_SHARED {
	// 	for _, v := range l.connections {
	// 		if v >= C.SQLITE_LOCK_PENDING {
	// 			return false
	// 		}
	// 	}
	// }

	// // A reserved lock can be acquired as long as there are no other connections
	// // with reserved, exclusive or pending locks.
	// if lockType == C.SQLITE_LOCK_RESERVED {
	// 	for k, v := range l.connections {
	// 		if v >= C.SQLITE_LOCK_RESERVED && k != connectionId {
	// 			return false
	// 		}
	// 	}
	// }

	// // A pending lock can be acquired as long as there are no other connections
	// // with exclusive locks.
	// if lockType == C.SQLITE_LOCK_PENDING {
	// 	for _, v := range l.connections {
	// 		if v >= C.SQLITE_LOCK_PENDING {
	// 			return false
	// 		}
	// 	}
	// }

	for k, v := range l.connections {
		if lockType == C.SQLITE_LOCK_EXCLUSIVE && v > C.SQLITE_LOCK_NONE && k != connectionId {
			return false
		}
		if lockType == C.SQLITE_LOCK_SHARED && v >= C.SQLITE_LOCK_PENDING {
			return false
		}
		if lockType == C.SQLITE_LOCK_RESERVED && v >= C.SQLITE_LOCK_RESERVED && k != connectionId {
			return false
		}
		if lockType == C.SQLITE_LOCK_PENDING && v >= C.SQLITE_LOCK_PENDING {
			return false
		}
	}

	l.connections[connectionId] = lockType

	return true
}

func (l *VfsLock) Unlock(connectionId string, lockType int) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.connections[connectionId] = lockType
}
