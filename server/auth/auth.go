package auth

import (
	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/storage"
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
	networkFS *storage.FileSystem,
	objectFS *storage.FileSystem,
	tmpFS *storage.FileSystem,
	tmpTieredFS *storage.FileSystem,
) *Auth {
	auth := &Auth{
		Config:   c,
		ObjectFS: objectFS,
		TmpFS:    tmpFS,
	}

	auth.SecretsManager = NewSecretsManager(
		auth,
		c,
		networkFS,
		objectFS,
		tmpFS,
		tmpTieredFS,
	)

	auth.AccessKeyManager = NewAccessKeyManager(auth, auth.Config, objectFS)

	return auth
}
