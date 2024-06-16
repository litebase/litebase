package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/storage"
	"log"
	"net/http"
	"os"
	"strings"
)

var NodeIpAddress string
var NodeIPv6Address string

func DirectoryPath() string {
	return fmt.Sprintf("%s/nodes/query", config.Get().DataPath)
}

func FilePath(ipAddress string) string {
	if ipAddress == "" {
		ipAddress = GetPrivateIpAddress()
	}

	return fmt.Sprintf("%s/%s_%s", DirectoryPath(), ipAddress, config.Get().QueryNodePort)
}

func GetPrivateIpAddress() string {
	if NodeIpAddress != "" {
		return NodeIpAddress
	}

	if config.Get().Env == "local" || config.Get().Env == "testing" {
		// NodeIpAddress = "127.0.0.1"
		NodeIpAddress, err := os.Hostname()

		if err != nil {
			log.Fatal(err)
		}

		return NodeIpAddress
	}

	url := fmt.Sprintf("%s/task", os.Getenv("ECS_CONTAINER_METADATA_URI_V4"))

	res, err := http.Get(url)

	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()

	body, readErr := io.ReadAll(res.Body)

	if readErr != nil {
		log.Fatal(readErr)
	}

	var data map[string]interface{}

	jsonErr := json.Unmarshal(body, &data)

	if jsonErr != nil {
		log.Fatal(jsonErr)
	}

	NodeIpAddress = data["Containers"].([]interface{})[0].(map[string]interface{})["Networks"].([]interface{})[0].(map[string]interface{})["IPv4Addresses"].([]interface{})[0].(string)

	log.Printf("Node IP Address: %s \n", NodeIpAddress)

	return NodeIpAddress
}

func GetIPv6Address() string {
	if NodeIPv6Address != "" {
		return NodeIPv6Address
	}

	if config.Get().Env == "local" || config.Get().Env == "testing" {
		return fmt.Sprintf("localhost:%s", config.Get().QueryNodePort)
	}

	url := "http://169.254.170.2/v2/metadata"

	res, err := http.Get(url)

	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()

	body, readErr := io.ReadAll(res.Body)

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
	if _, err := storage.FS().Stat(FilePath(ip)); err == nil {
		return true
	}

	return false
}

func HealthCheck() {
	path := DirectoryPath()
	nodes := Instances()

	for _, node := range nodes {
		if node == GetPrivateIpAddress()+"_"+config.Get().QueryNodePort {
			continue
		}

		ip := strings.Split(node, "_")[0]
		port := strings.Split(node, "_")[1]

		go func(ip, port string) {
			url := fmt.Sprintf("http://%s:%s", ip, port)

			res, err := http.Get(url)

			if err != nil {
				log.Println(err)
				ipPath := fmt.Sprintf("%s/%s_%s", path, ip, port)

				if _, err := storage.FS().Stat(ipPath); err == nil {
					storage.FS().Remove(ipPath)
				}

				return
			}

			defer res.Body.Close()
		}(ip, port)
	}
}

func Init() {
	path := DirectoryPath()

	// Make directory if it doesn't exist
	if storage.FS().Stat(path); os.IsNotExist(os.ErrNotExist) {
		storage.FS().Mkdir(path, 0755)
	}
}

func Instances() []string {
	path := DirectoryPath()

	// Check if the directory exists
	if _, err := storage.FS().Stat(path); os.IsNotExist(err) {
		return []string{}
	}

	// Read the directory
	files, err := storage.FS().ReadDir(path)

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

func OtherNodes() []*NodeIdentifier {
	ips := Instances()
	nodes := []*NodeIdentifier{}

	for _, ip := range ips {
		if ip == GetPrivateIpAddress()+"_"+config.Get().QueryNodePort {
			continue
		}

		nodes = append(nodes, &NodeIdentifier{
			IP:   strings.Split(ip, "_")[0],
			Port: strings.Split(ip, "_")[1],
		})
	}

	return nodes
}

func PurgeDatabaseSettings(databaseUuid string) {
	nodes := OtherNodes()

	for _, node := range nodes {
		go func(node *NodeIdentifier) {
			url := fmt.Sprintf("http://%s:%s/databases/%s/settings/purge", node.IP, node.Port, databaseUuid)
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
				log.Println(res)
			}
		}(node)
	}
}

// Store the node ip in the nodes directory
func Register() {
	ipAddress := GetPrivateIpAddress()
	filePath := FilePath(ipAddress)

	if !Has(ipAddress) {
	write:
		err := storage.FS().WriteFile(filePath, []byte(ipAddress), 0644)

		if err != nil {
			if os.IsNotExist(err) {
				storage.FS().MkdirAll(DirectoryPath(), 0755)
				goto write
			}

			log.Println(err)
		}
	}
}

func SendEvent(node *NodeIdentifier, message NodeEvent) {
	url := fmt.Sprintf("http://%s:%s/events", node.IP, node.Port)
	data, err := json.Marshal(message)

	if err != nil {
		log.Println(err)
		return
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))

	if err != nil {
		log.Println(err)
		return
	}

	encryptedHeader, err := auth.SecretsManager().Encrypt(
		config.Get().Signature,
		GetPrivateIpAddress(),
	)

	if err != nil {
		log.Println(err)
		return
	}

	req.Header.Set("X-Lbdb-Node", encryptedHeader)

	client := &http.Client{}

	res, err := client.Do(req)

	if err != nil {
		log.Println(err)
		return
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Println(res)
	}
}

func Unregister() {
	ipAddress := GetPrivateIpAddress()
	filePath := FilePath(ipAddress)

	if Has(ipAddress) {
		err := storage.FS().Remove(filePath)

		if err != nil {
			log.Println(err)
		}
	}

	fmt.Println("↳ Node unregistered successfully")
}
