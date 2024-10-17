package auth

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"litebase/server/storage"
	"log"
	"time"
)

/*
The FileSecretsStore is a secrets store that stores secrets in files on disk.
Thsese files are stored in a temporary directory and should be deleted when
the application is closed.
*/
type FileSecretsStore struct {
	path  string
	tmpFS *storage.FileSystem
}

type FileSecret struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

// TODO: This should be using the local file system abstraction
func NewFileSecretsStore(path string, tmpFS *storage.FileSystem) *FileSecretsStore {
	return &FileSecretsStore{
		path:  path,
		tmpFS: tmpFS,
	}
}

func (store *FileSecretsStore) Flush() {
	store.tmpFS.RemoveAll(store.path)
}

func (store *FileSecretsStore) Forget(key string) {
	store.tmpFS.Remove(fmt.Sprintf("%s/%s", store.path, store.Key(key)))
}

func (store *FileSecretsStore) Get(key string, cacheItemType interface{}) interface{} {
	data, err := store.tmpFS.ReadFile(fmt.Sprintf("%s/%s", store.path, store.Key(key)))
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

	if err := json.Unmarshal([]byte(secret.Value), &cacheItemType); err != nil {
		return nil
	}

	return cacheItemType
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
		ExpiresAt: time.Now().Add(seconds),
	}

	jsonValue, err = json.Marshal(secret)

	if err != nil {
		log.Fatal(err)
	}

	err = store.tmpFS.WriteFile(store.path+"/"+store.Key(key), jsonValue, 0666)

	return err == nil
}
