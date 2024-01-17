package auth

import (
	"encoding/json"
	"sync"
	"time"
)

type MapSecretsStore struct {
	data  map[string]MapSecret
	mutex *sync.Mutex
}

type MapSecret struct {
	value     string
	expiresAt time.Time
}

func NewMapSecretsStore() *MapSecretsStore {
	return &MapSecretsStore{
		data:  make(map[string]MapSecret),
		mutex: &sync.Mutex{},
	}
}

func (store *MapSecretsStore) Flush() {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.data = make(map[string]MapSecret)
}

func (store *MapSecretsStore) Forget(key string) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	delete(store.data, key)
}

func (store *MapSecretsStore) Get(key string) interface{} {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if secret, ok := store.data[key]; ok {

		if time.Now().After(secret.expiresAt) {
			store.Forget(key)
			return nil
		}

		jsonValue := interface{}(nil)

		if err := json.Unmarshal([]byte(secret.value), &jsonValue); err != nil {
			return nil
		}

		return jsonValue
	}

	return nil
}

func (store *MapSecretsStore) Put(key string, value any, seconds time.Duration) bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	jsonValue, err := json.Marshal(value)

	if err != nil {
		return false
	}

	store.data[key] = MapSecret{
		value:     string(jsonValue),
		expiresAt: time.Now().Add(time.Second * seconds),
	}

	return true
}
