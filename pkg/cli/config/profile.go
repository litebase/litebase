package config

type Profile struct {
	Name        string             `json:"name"`
	Cluster     string             `json:"cluster"`
	Credentials ProfileCredentials `json:"credentials"`
	Type        ProfileType        `json:"type"`
}

type ProfileType string

const (
	ProfileTypeBasicAuth string = "basicAuth"
	ProfileTypeAccessKey string = "accessKey"
)

type ProfileCredentials struct {
	Username        string `json:"username"`
	Password        string `json:"password"`
	AccessKeyId     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
}
