package auth

type Auth struct {
	accessKeyManager *AccessKeyManager
	secretsManager   *SecretsManager
}

func NewAuth() *Auth {
	return &Auth{}
}
