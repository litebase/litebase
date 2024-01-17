package api

func Get(path string) (map[string]interface{}, error) {
	data, _, err := NewClient().Request("GET", path, nil)

	return data, err
}

func Post(path string, body map[string]interface{}) (map[string]interface{}, Errors, error) {
	return NewClient().Request("POST", path, body)
}

func Delete(path string) (map[string]interface{}, Errors, error) {
	return NewClient().Request("DELETE", path, nil)
}
