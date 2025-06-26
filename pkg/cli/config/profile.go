package config

type Profile struct {
	Name        string             `json:"name"`
	Cluster     string             `json:"cluster"`
	Credentials ProfileCredentials `json:"credentials"`
	Type        string             `json:"type"`
}

type ProfileType string

const (
	ProfileTypeBasicAuth ProfileType = "basic_auth"
	ProfileTypeAccessKey ProfileType = "access_key"
)

type ProfileCredentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	AccessKeyID     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
}
