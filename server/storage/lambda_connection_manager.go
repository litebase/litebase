package storage

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
)

type LambdaConnectionManagerInstance struct {
	mutext *sync.RWMutex
	pools  map[string]*LambdaConnectionPool
}

var StaticLambdaConnectionManagerInstance *LambdaConnectionManagerInstance

func LambdaConnectionManager() *LambdaConnectionManagerInstance {
	if StaticLambdaConnectionManagerInstance == nil {
		StaticLambdaConnectionManagerInstance = &LambdaConnectionManagerInstance{
			mutext: &sync.RWMutex{},
			pools:  map[string]*LambdaConnectionPool{},
		}
	}

	return StaticLambdaConnectionManagerInstance
}

func (lcm *LambdaConnectionManagerInstance) Activate(connectionHash string, connection *LambdaConnection, w http.ResponseWriter, r *http.Request) error {
	lcm.mutext.RLock()
	pool, ok := lcm.pools[connectionHash]
	lcm.mutext.RUnlock()

	if ok {
		return pool.Activate(connection, w, r)
	}

	return errors.New("no pool found")
}

// Create
func (lcm *LambdaConnectionManagerInstance) Create(connectionHash string, connectionUrl string, capacity int) *LambdaConnectionPool {
	lcm.pools[connectionHash] = NewLambdaConnectionPool(capacity, connectionHash, connectionUrl)

	return lcm.pools[connectionHash]
}

// Get
func (lcm *LambdaConnectionManagerInstance) Get(connectionHash string) (*LambdaConnection, error) {
	lcm.mutext.Lock()
	pool, ok := lcm.pools[connectionHash]

	if !ok {
		pool = lcm.Create(
			connectionHash,
			fmt.Sprintf("http://localhost:8081/storage/%s/connections", connectionHash),
			// fmt.Sprintf("http://host.docker.internal:8081/storage/%s/connections", connectionHash),
			1,
		)
	}
	lcm.mutext.Unlock()

	return pool.Get()
}

func (lcm *LambdaConnectionManagerInstance) Find(connectionHash string, connectionId string) (*LambdaConnection, error) {
	lcm.mutext.RLock()
	pool, ok := lcm.pools[connectionHash]
	lcm.mutext.RUnlock()

	if !ok {
		return nil, errors.New("no pool found")
	}

	return pool.Find(connectionId)
}

// Release
func (lcm *LambdaConnectionManagerInstance) Release(connectionHash string, connection *LambdaConnection) {
	lcm.mutext.RLock()
	pool, ok := lcm.pools[connectionHash]
	lcm.mutext.RUnlock()

	if ok {
		pool.Release(connection)
	}
}

// Remove
func (lcm *LambdaConnectionManagerInstance) Remove(connectionHash string, connection *LambdaConnection) {
	lcm.mutext.Lock()
	pool, ok := lcm.pools[connectionHash]
	lcm.mutext.Unlock()

	if ok {
		pool.Remove(connection)
	}

	// TODO: Remove pool if empty
}
