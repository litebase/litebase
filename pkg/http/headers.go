package http

import "github.com/litebase/litebase/internal/utils"

type Headers struct {
	values map[string]string
}

func NewHeaders(headers map[string]string) Headers {
	h := make(map[string]string, len(headers))

	for key, value := range headers {
		h[utils.TransformHeaderKey(key)] = value
	}

	return Headers{values: h}
}

func (headers Headers) All() map[string]string {
	return headers.values
}

func (headers Headers) Get(key string) string {
	return headers.values[utils.TransformHeaderKey(key)]
}

func (headers Headers) Has(key string) bool {
	_, ok := headers.values[utils.TransformHeaderKey(key)]

	return ok
}
