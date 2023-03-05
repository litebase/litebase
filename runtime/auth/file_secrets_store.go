package auth

import (
	"crypto/sha1"
	"encoding/json"
	"os"
	"time"
)

type FileSecretsStore struct {
	path string
}

type FileSecret struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

func NewFileSecretsStore(path string) *FileSecretsStore {
	return &FileSecretsStore{path}
}

func (store *FileSecretsStore) Flush() {
	os.RemoveAll(store.path)
}

func (store *FileSecretsStore) Forget(key string) {
	os.Remove(store.path + "/" + store.Key(key))
}

func (store *FileSecretsStore) Get(key string) interface{} {
	data, err := os.ReadFile(store.path + "/" + store.Key(key))
	secret := FileSecret{}

	if err != nil {
		return nil
	}

	if len(data) == 0 {
		return nil
	}

	json.Unmarshal(data, &secret)

	if time.Now().After(secret.ExpiresAt) {
		store.Forget(key)
		return nil
	}

	jsonValue := interface{}(nil)

	if err := json.Unmarshal([]byte(secret.Value), &jsonValue); err != nil {
		return nil
	}

	return jsonValue
}

// Key Function for the sha1 hash
func (store *FileSecretsStore) Key(key string) string {
	sha1 := sha1.New()
	sha1.Write([]byte(key))
	return string(sha1.Sum(nil))
}

func (store *FileSecretsStore) Put(key string, value any, seconds time.Duration) bool {
	jsonValue, err := json.Marshal(value)

	if err != nil {
		return false
	}

	secret := FileSecret{
		Value:     string(jsonValue),
		ExpiresAt: time.Now().Add(time.Second * seconds),
	}

	jsonValue, err = json.Marshal(secret)

	if err != nil {
		return false
	}

	err = os.WriteFile(store.path+"/"+store.Key(key), jsonValue, 0666)

	return err == nil
}
