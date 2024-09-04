//go:build !production
// +build !production

package storage

import (
	"litebase/internal/config"
	"log"
)

func Init() {
	storageMode := config.Get().StorageMode

	if storageMode == "local" {
		return
	}

	if storageMode == "object" && (config.Get().Env == "local" || config.Get().Env == "test") {
		StartTestS3Server()
		return
	}
}

func Shutdown() {
	log.Println("Shutting down storage")

	StopTestS3Server()
}
