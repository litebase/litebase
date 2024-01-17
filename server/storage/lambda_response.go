package storage

type DatabaseResponse struct {
	Id    string `json:"id"`
	Size  int64  `json:"size"`
	Pages []Page `json:"pages"`
}

type FilesystemResponse struct {
	Path       string                   `json:"path"`
	Bytes      int64                    `json:"bytes"`
	Data       []byte                   `json:"data"`
	DirEntries []map[string]interface{} `json:"dirEntries"`
	Error      string                   `json:"error"`
	Stat       map[string]interface{}   `json:"stat"`
	TotalBytes int64                    `json:"totalBytes"`
}

type Page struct {
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Data   []byte `json:"data"`
	Error  string `json:"error"`
}

type Response struct {
	Id                 string             `json:"id"`
	DatabaseResponse   DatabaseResponse   `json:"databaseResponse"`
	FilesystemResponse FilesystemResponse `json:"filesystemResponse"`
}
