package auth

import "time"

type SecretsStore interface {
	Flush()
	Forget(key string)
	Get(key string, cacheItemType interface{}) interface{}
	Put(key string, value any, seconds time.Duration) bool
}
