package http

import (
	"litebasedb/internal/utils"
)

type Headers struct {
	values map[string]string
}

func NewHeaders(headers map[string]string) *Headers {
	for key, value := range headers {
		delete(headers, key)
		headers[utils.TransformHeaderKey(key)] = value
	}

	return &Headers{values: headers}
}

func (headers *Headers) All() map[string]string {
	return headers.values
}

func (headers *Headers) Get(key string) string {
	return headers.values[utils.TransformHeaderKey(key)]
}

func (headers *Headers) Has(key string) bool {
	_, ok := headers.values[utils.TransformHeaderKey(key)]

	return ok
}
