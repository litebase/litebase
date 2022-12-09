package auth

type AccessKey struct {
	DatabaseUuid          string `json:"database_uuid"`
	BranchUuid            string `json:"branch_uuid"`
	AccessKeyId           string `json:"access_key_id"`
	ServerAccessKeySecret string `json:"server_access_key_secret"`
}

func (accessKey *AccessKey) GetAccessKeyId() string {
	return accessKey.AccessKeyId
}

func (accessKey *AccessKey) GetBranchUuid() string {
	return accessKey.BranchUuid
}

func (accessKey *AccessKey) GetDatabaseUuid() string {
	return accessKey.DatabaseUuid
}
