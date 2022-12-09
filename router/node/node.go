package node

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"litebasedb/router/config"
	"log"
	"net/http"
	"os"
	"strings"
)

var NodeIpAddress string
var NodeIPv6Address string

func DirectoryPath() string {
	return config.Get("dataPath") + "/nodes"
}

func FilePath(ipAddress string) string {
	if ipAddress == "" {
		ipAddress = GetPrivateIpAddress()
	}

	return fmt.Sprintf("%s/%s:%s.json", DirectoryPath(), ipAddress, config.Get("port"))
}

func GetPrivateIpAddress() string {
	if NodeIpAddress != "" {
		return NodeIpAddress
	}

	if config.Get("env") == "local" || config.Get("env") == "testing" {
		NodeIpAddress = "127.0.0.1"

		return NodeIpAddress
	}

	url := fmt.Sprintf("%s/task", os.Getenv("ECS_CONTAINER_METADATA_URI_V4"))

	res, err := http.Get(url)

	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()

	body, readErr := ioutil.ReadAll(res.Body)

	if readErr != nil {
		log.Fatal(readErr)
	}

	var data map[string]interface{}

	jsonErr := json.Unmarshal(body, &data)

	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	NodeIpAddress = data["Containers"].([]interface{})[0].(map[string]interface{})["Networks"].([]interface{})[0].(map[string]interface{})["IPv4Addresses"].([]interface{})[0].(string)

	log.Println(fmt.Sprintf("Node IP Address: %s", NodeIpAddress))

	return NodeIpAddress
}

func GetIPv6Address() string {
	if NodeIPv6Address != "" {
		return NodeIPv6Address
	}

	if config.Get("env") == "local" || config.Get("env") == "testing" {
		return "::1"
	}

	url := "http://169.254.170.2/v2/metadata"

	res, err := http.Get(url)

	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()

	body, readErr := ioutil.ReadAll(res.Body)

	if readErr != nil {
		log.Fatal(readErr)
	}

	var data map[string]interface{}

	jsonErr := json.Unmarshal(body, &data)

	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	NodeIPv6Address = data["Containers"].([]interface{})[0].(map[string]interface{})["Networks"].([]interface{})[0].(map[string]interface{})["IPv6Addresses"].([]interface{})[0].(string)

	return NodeIPv6Address
}

func Has(ip string) bool {
	if _, err := os.Stat(FilePath(ip)); err == nil {
		return true
	}

	return false
}

func HealthCheck() {
	path := DirectoryPath()
	ips := Instances()

	for _, ip := range ips {
		if ip == GetPrivateIpAddress()+":"+config.Get("port") {
			continue
		}

		go func(ip string) {
			url := fmt.Sprintf("http://%s", ip)

			res, err := http.Get(url)

			if err != nil {
				log.Println(err)
				ipPath := fmt.Sprintf("%s/%s.json", path, ip)

				if _, err := os.Stat(ipPath); err == nil {
					os.Remove(ipPath)
				}

				return
			}

			defer res.Body.Close()
		}(ip)
	}
}

func Instances() []string {
	path := DirectoryPath()

	// Check if the directory exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return []string{}
	}

	// Read the directory
	files, err := os.ReadDir(path)

	if err != nil {
		return []string{}
	}

	// Loop through the files
	instances := []string{}

	for _, file := range files {
		instances = append(instances, strings.ReplaceAll(file.Name(), ".json", ""))
	}

	return instances
}

func PurgeDatabaseSettings(databaseUuid string) {
	ips := Instances()

	for _, ip := range ips {
		if ip == GetPrivateIpAddress()+":"+config.Get("port") {
			continue
		}

		go func(ip string) {
			url := fmt.Sprintf("http://%s/databases/%s/settings/purge", ip, databaseUuid)
			req, err := http.NewRequest("POST", url, nil)

			if err != nil {
				log.Println(err)
				return
			}

			client := &http.Client{}

			res, err := client.Do(req)

			if err != nil {
				log.Println(err)
				return
			}

			defer res.Body.Close()

			if res.StatusCode != 200 {

			}
		}(ip)
	}
}
