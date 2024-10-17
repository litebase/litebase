package auth

import (
	"litebase/internal/config"
	"litebase/server/storage"
)

type Auth struct {
	AccessKeyManager *AccessKeyManager
	Config           *config.Config
	ObjectFS         *storage.FileSystem
	SecretsManager   *SecretsManager
	TmpFS            *storage.FileSystem
	userManager      *UserManager
}

func NewAuth(
	c *config.Config,
	objectFS *storage.FileSystem,
	tmpFS *storage.FileSystem,
) *Auth {
	auth := &Auth{
		Config:   c,
		ObjectFS: objectFS,
		TmpFS:    tmpFS,
	}

	auth.SecretsManager = NewSecretsManager(
		c,
		objectFS,
		tmpFS,
	)

	auth.AccessKeyManager = NewAccessKeyManager(auth, auth.Config, objectFS)

	return auth
}
