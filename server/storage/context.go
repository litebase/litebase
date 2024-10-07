package storage

import (
	"context"
	"time"
)

var storageContext context.Context
var storageTimestamp time.Time

func GetStorageContext() context.Context {
	if storageContext == nil {
		storageContext = context.Background()
	}

	return storageContext
}

func SetStorageContext(ctx context.Context) {
	storageContext = ctx
}

func SetStorageTimestamp(timestamp time.Time) {
	storageTimestamp = timestamp
}
