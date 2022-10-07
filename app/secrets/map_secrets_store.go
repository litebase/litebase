package secrets

import (
	"encoding/json"
	"time"
)

type MapSecretsStore struct {
	data map[string]MapSecret
}

type MapSecret struct {
	value     string
	expiresAt time.Time
}

func NewMapSecretsStore() *MapSecretsStore {
	return &MapSecretsStore{
		data: make(map[string]MapSecret),
	}
}

func (store *MapSecretsStore) Flush() {
	store.data = make(map[string]MapSecret)
}

func (store *MapSecretsStore) Forget(key string) {
	delete(store.data, key)
}

func (store *MapSecretsStore) Get(key string) interface{} {
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
