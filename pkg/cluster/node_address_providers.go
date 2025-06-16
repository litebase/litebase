package cluster

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
)

type NodeAddressProviderKey string
type NodeAddressProvider func() (string, error)

const (
	NodeAddressProviderKeyAWSECS NodeAddressProviderKey = "aws_ecs"
)

var nodeAddressProviders = map[NodeAddressProviderKey]NodeAddressProvider{
	NodeAddressProviderKeyAWSECS: AWSECSAddressProvider,
}

// Get private ip address of the node from ECS_CONTAINER_METADATA_URI_V4
func AWSECSAddressProvider() (string, error) {
	metaDataUri := os.Getenv("ECS_CONTAINER_METADATA_URI_V4")

	if metaDataUri == "" {
		return "", errors.New("ECS_CONTAINER_METADATA_URI_V4 not set")
	}

	res, err := http.Get(metaDataUri + "/task")

	if err != nil {
		return "", err
	}

	if res.StatusCode != http.StatusOK {
		return "", errors.New("failed to get metadata from ECS")
	}

	defer res.Body.Close()
	data := map[string]any{}

	decoder := json.NewDecoder(res.Body)

	err = decoder.Decode(&data)

	if err != nil {
		return "", err
	}

	// Read  the value from Containers[0].Networks[0].IPv4Addresses[0]

	containers, ok := data["Containers"].([]any)

	if !ok || len(containers) == 0 {
		return "", errors.New("failed to get containers from metadata")
	}

	// Read the first container
	container, ok := containers[0].(map[string]any)

	if !ok {
		return "", errors.New("failed to get container from metadata")
	}

	networks, ok := container["Networks"].([]any)

	if !ok || len(networks) == 0 {
		return "", errors.New("failed to get networks from metadata")
	}

	// Read the first network
	network, ok := networks[0].(map[string]any)

	if !ok {
		return "", errors.New("failed to get network from metadata")
	}

	ipv4Addresses, ok := network["IPv4Addresses"].([]any)

	if !ok || len(ipv4Addresses) == 0 {
		return "", errors.New("failed to get ipv4 addresses from metadata")
	}

	// Read the first ipv4 address
	ipv4Address, ok := ipv4Addresses[0].(string)

	if !ok {
		return "", errors.New("failed to get ipv4 address from metadata")
	}

	return ipv4Address, nil
}
