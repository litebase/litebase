package api

import "github.com/litebase/litebase/pkg/cli/config"

func Get(config *config.CLIConfiguration, path string) (map[string]any, error) {
	client, err := NewClient(config)

	if err != nil {
		return nil, err
	}

	data, _, err := client.Request("GET", path, nil)

	return data, err
}

func Post(config *config.CLIConfiguration, path string, body map[string]any) (map[string]any, Errors, error) {
	client, err := NewClient(config)

	if err != nil {
		return nil, nil, err
	}

	return client.Request("POST", path, body)
}

func Delete(config *config.CLIConfiguration, path string) (map[string]any, Errors, error) {
	client, err := NewClient(config)

	if err != nil {
		return nil, nil, err
	}

	return client.Request("DELETE", path, nil)
}

func Patch(config *config.CLIConfiguration, path string, body map[string]any) (map[string]any, Errors, error) {
	client, err := NewClient(config)

	if err != nil {
		return nil, nil, err
	}

	return client.Request("PATCH", path, body)
}

func Put(config *config.CLIConfiguration, path string, body map[string]any) (map[string]any, Errors, error) {
	client, err := NewClient(config)

	if err != nil {
		return nil, nil, err
	}

	return client.Request("PUT", path, body)
}
