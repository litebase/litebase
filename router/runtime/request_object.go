package runtime

type RuntimeRequestObject struct {
	AccessKeyId  string
	Body         map[string]interface{}
	BranchUuid   string
	DatabaseUuid string
	Headers      map[string]string
	Host         string
	Method       string
	Path         string
	Query        map[string]string
	Signature    string
}
