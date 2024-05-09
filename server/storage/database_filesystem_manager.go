package storage

import "sync"

type DatabaseFileSystemManagerInstance struct {
	filesystems map[string]DatabaseFileSystem
	mutex       *sync.RWMutex
}

var StaticDatabaseFileSystemManager *DatabaseFileSystemManagerInstance

func DatabaseFileSystemManager() *DatabaseFileSystemManagerInstance {
	if StaticDatabaseFileSystemManager == nil {
		StaticDatabaseFileSystemManager = &DatabaseFileSystemManagerInstance{
			mutex:       &sync.RWMutex{},
			filesystems: map[string]DatabaseFileSystem{},
		}
	}

	return StaticDatabaseFileSystemManager
}

func (dfsm *DatabaseFileSystemManagerInstance) LambdaDatabaseFileSystem(connectionHash string, tmpPath string, databaseUuid string, branchUuid string, pageSize int64) DatabaseFileSystem {
	dfsm.mutex.Lock()
	fs, ok := dfsm.filesystems[connectionHash]

	if !ok {
		fs = NewLambdaDatabaseFileSystem(connectionHash, tmpPath, databaseUuid, branchUuid, pageSize)
		dfsm.filesystems[connectionHash] = fs
	}

	dfsm.mutex.Unlock()

	return fs
}
