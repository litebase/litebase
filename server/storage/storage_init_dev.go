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
		log.Println("local storage mode")
		return
	}

	if storageMode == "object" && (config.Get().Env == "local" || config.Get().Env == "test") {
		log.Println("tiered storage mode")

		StartTestS3Server()
		return
	}

	log.Println("storage package initialized")
}
