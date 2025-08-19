package auth

import (
	"fmt"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

type Auth struct {
	AccessKeyManager *AccessKeyManager
	Config           *config.Config
	ObjectFS         *storage.FileSystem
	SecretsManager   *SecretsManager
	TmpFS            *storage.FileSystem
	UserManager      *UserManager
	TokenManager     *TokenManager

	broadcaster func(key string, value string)
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

	return auth
}

// Broadcast an auth event to all listeners.
func (a *Auth) Broadcast(key string, value string) {
	if a.broadcaster != nil {
		a.broadcaster(key, value)
	}
}

// Set a broadcaster function for auth events.
func (a *Auth) Broadcaster(f func(key string, value string)) {
	a.broadcaster = f
}

// Get the credential by ID and scheme.
func (a *Auth) GetCredential(id string, scheme string) (*Credential, error) {
	switch scheme {
	case "Basic":
		if user, err := a.UserManager.Get(id); err == nil {
			credential := &Credential{}

			return credential.WithUser(user), nil
		}
	case "Bearer":
		if token, err := a.TokenManager.Get(id); err == nil {
			credential := &Credential{}

			return credential.WithToken(token), nil
		}
	case "Litebase-HMAC-SHA256":
		if accessKey, err := a.AccessKeyManager.Get(id); err == nil {
			credential := &Credential{}

			return credential.WithAccessKey(accessKey), nil
		}
	}

	return nil, fmt.Errorf("unsupported credential scheme: %s", scheme)
}

// Provide the interface that will manage access key storage and create the
// AccessKeyManager instance.
func (a *Auth) ProvideAccessKeyStorage(accessKeyStorage AccessKeyStorage) {
	a.AccessKeyManager = NewAccessKeyManager(
		accessKeyStorage,
		a,
		a.Config,
	)
}

// Provide the interface that will manage token storage and create the
// TokenManager instance.
func (a *Auth) ProvideTokenStorage(tokenStorage TokenStorage) {
	a.TokenManager = NewTokenManager(
		tokenStorage,
		a,
	)
}

// Provide the interface that will manage access key storage and create the
// AccessKeyManager instance.
func (a *Auth) ProvideUserManagerStorage(userStorage UserStorage) {
	a.UserManager = NewUserManager(
		userStorage,
		a,
		a.Config,
	)
}
