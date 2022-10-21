package http

import "litebasedb/runtime/internal/utils"

type Headers struct {
	values map[string]string
}

func NewHeaders(headers map[string]string) *Headers {
	// values := map[string]string{}

	// // Split the header string into an array of header strings
	// headerStrings := strings.Split(headerString, "\r, \n")

	// // Iterate over the header strings
	// for _, headerString := range headerStrings {
	// 	// Split the header string into an array of key and value strings
	// 	keyValueStrings := strings.Split(headerString, ": ")

	// 	// Set the value of the header
	// 	values[keyValueStrings[0]] = keyValueStrings[1]
	// }

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
