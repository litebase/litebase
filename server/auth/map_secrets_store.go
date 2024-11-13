package auth

import (
	"encoding/json"
	"sync"
	"time"
)

type MapSecretsStore struct {
	data  map[string]MapSecret
	mutex *sync.RWMutex
}

type MapSecret struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

func NewMapSecretsStore() *MapSecretsStore {
	return &MapSecretsStore{
		data:  make(map[string]MapSecret),
		mutex: &sync.RWMutex{},
	}
}

func (store *MapSecretsStore) Flush() error {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.data = make(map[string]MapSecret)

	return nil
}

func (store *MapSecretsStore) Forget(key string) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	delete(store.data, key)
}

func (store *MapSecretsStore) Get(key string, cacheItemType interface{}) interface{} {
	store.mutex.RLock()
	secret, ok := store.data[key]
	store.mutex.RUnlock()

	if !ok {
		return nil
	}

	if time.Now().After(secret.ExpiresAt) {
		store.Forget(key)
		return nil
	}

	if err := json.Unmarshal([]byte(secret.Value), cacheItemType); err != nil {
		return nil
	}

	return cacheItemType
}

func (store *MapSecretsStore) Put(key string, value interface{}, seconds time.Duration) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	jsonValue, err := json.Marshal(value)

	if err != nil {
		return false
	}

	store.data[key] = MapSecret{
		Value:     string(jsonValue),
		ExpiresAt: time.Now().Add(seconds),
	}

	return true
}
