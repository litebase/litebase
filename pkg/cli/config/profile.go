package config

type Profile struct {
	Name        string             `yaml:"name"`
	Cluster     string             `yaml:"cluster"`
	Credentials ProfileCredentials `yaml:"credentials"`
	Type        string             `yaml:"type"`
}

type ProfileType string

const (
	ProfileTypeBasicAuth ProfileType = "basic_auth"
	ProfileTypeAccessKey ProfileType = "access_key"
	ProfileTypeToken     ProfileType = "token"
)

type ProfileCredentials struct {
	Username        string `yaml:"username,omitempty"`
	Password        string `yaml:"password,omitempty"`
	AccessKeyID     string `yaml:"accessKeyId,omitempty"`
	AccessKeySecret string `yaml:"accessKeySecret,omitempty"`
	Token           string `yaml:"token,omitempty"`
}
