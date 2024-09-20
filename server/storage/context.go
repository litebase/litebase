package storage

import "context"

var storageContext context.Context

func GetStorageContext() context.Context {
	if storageContext == nil {
		storageContext = context.Background()
	}

	return storageContext
}

func SetStorageContext(ctx context.Context) {
	storageContext = ctx
}
