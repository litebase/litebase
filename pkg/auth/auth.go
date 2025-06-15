package auth

import (
	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/pkg/storage"
)

type Auth struct {
	AccessKeyManager *AccessKeyManager
	Config           *config.Config
	ObjectFS         *storage.FileSystem
	SecretsManager   *SecretsManager
	TmpFS            *storage.FileSystem

	broadcaster func(key string, value string)
	userManager *UserManager
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

// Broaddcast a an auth event to all listeners.
func (a *Auth) Broadcast(key string, value string) {
	if a.broadcaster != nil {
		a.broadcaster(key, value)
	}
}

// Set a broadcaster function for auth events.
func (a *Auth) Broadcaster(f func(key string, value string)) {
	a.broadcaster = f
}
