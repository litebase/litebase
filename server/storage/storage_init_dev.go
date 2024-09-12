//go:build !production
// +build !production

package storage

import (
	"litebase/internal/config"
)

func Init() {
	storageMode := config.Get().StorageMode

	if storageMode == "local" {
		return
	}

	if storageMode == "object" && (config.Get().Env == "local") {
		StartTestS3Server()
		return
	}
}

func Shutdown() {
	storageMode := config.Get().StorageMode

	if storageMode == "object" && (config.Get().Env == "local") {
		StopTestS3Server()
	}
}
