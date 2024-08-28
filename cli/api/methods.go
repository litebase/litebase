package api

func Get(path string) (map[string]interface{}, error) {
	client, err := NewClient()

	if err != nil {
		return nil, err
	}

	data, _, err := client.Request("GET", path, nil)

	return data, err
}

func Post(path string, body map[string]interface{}) (map[string]interface{}, Errors, error) {
	client, err := NewClient()

	if err != nil {
		return nil, nil, err
	}

	return client.Request("POST", path, body)
}

func Delete(path string) (map[string]interface{}, Errors, error) {
	client, err := NewClient()

	if err != nil {
		return nil, nil, err
	}

	return client.Request("DELETE", path, nil)
}
