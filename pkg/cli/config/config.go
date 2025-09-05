package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v4"
)

const CLIConfigurationVersion = "0.1"

type CLIConfiguration struct {
	APIVersion     string                 `yaml:"api_version"`
	CurrentProfile string                 `yaml:"current_profile"`
	Profiles       []Profile              `yaml:"profiles"`
	Server         CLIServerConfiguration `yaml:"server"`

	accessKeyId     string
	accessKeySecret string
	interactive     bool
	password        string
	path            string
	token           string
	url             string
	username        string
}

type CLIServerConfiguration struct {
	Background         bool   `yaml:"background"`
	ClusterID          string `yaml:"cluster_id"`
	ConfigPath         string `yaml:"config_path"`
	Debug              bool   `yaml:"debug,omitempty"`
	Key                string `yaml:"key"`
	Port               string `yaml:"port"`
	StoragePath        string `yaml:"storage_path"`
	StorageNetworkPath string `yaml:"storage_network_path"`
	StorageTmpPath     string `yaml:"storage_tmp_path"`
	TLSCertPath        string `yaml:"tls_cert_path"`
	TLSKeyPath         string `yaml:"tls_key_path"`
}

var ErrMissingClusterURL = errors.New("missing cluster URL")
var ErrorCredentialsNotSet = errors.New("credentials were not set, please provide access credentials or a stored profile name")
var ErrorProfileNotFound = errors.New("profile not found, provide a valid profile name")

// Create a new configuration instance. If the path is not provided, it defaults
// to ~/.litebase/config.yml which is a global configuration file.
func NewConfiguration(path string, create bool) (*CLIConfiguration, error) {
	configuration := &CLIConfiguration{
		APIVersion:  CLIConfigurationVersion,
		interactive: true,
	}

	if path == "" {
		homeDir, err := os.UserHomeDir()

		if err != nil {
			return nil, err
		}

		configuration.path = filepath.Join(homeDir, ".litebase", "config.yml")
	} else {
		configuration.path = path
	}

	if !create {
		yamlData, err := os.ReadFile(configuration.path)

		if err != nil && !os.IsNotExist(err) {
			return nil, err
		} else if err == nil {
			err = yaml.Unmarshal(yamlData, configuration)

			if err != nil {
				return nil, err
			}
		}
	}

	err := os.MkdirAll(filepath.Dir(configuration.path), 0750)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(configuration.path)

	if err != nil {
		if os.IsNotExist(err) && create {
			return configuration, configuration.Save()
		}
	}

	return configuration, nil
}

// Add a new profile to the configuration.
func (c *CLIConfiguration) AddProfile(profile Profile) error {
	c.Profiles = append(c.Profiles, profile)

	return c.Save()
}

func (c *CLIConfiguration) GetInteractive() bool {
	return c.interactive
}

// Get the profiles of the configuration.
func (c *CLIConfiguration) GetProfiles() []Profile {
	return c.Profiles
}

// Get a specific profile by name.
func (c *CLIConfiguration) GetProfile(name string) *Profile {
	for _, profile := range c.Profiles {
		if profile.Name == name {
			return &profile
		}
	}

	return nil
}

// Delete a profile from the configuration by name.
func (c *CLIConfiguration) DeleteProfile(name string) error {
	profiles := []Profile{}
	var profileFound bool

	for _, profile := range c.Profiles {
		if profile.Name != name {
			profiles = append(profiles, profile)
		} else {
			profileFound = true
		}
	}

	if !profileFound {
		return ErrorProfileNotFound
	}

	c.Profiles = profiles

	return c.Save()
}

// Return the access key ID used for authentication.
func (c *CLIConfiguration) GetAccessKeyId() string {
	return c.accessKeyId
}

// Return the access key secret used for authentication.
func (c *CLIConfiguration) GetAccessKeySecret() string {
	return c.accessKeySecret
}

// Return the current profile or the first profile if no current profile is set.
func (c *CLIConfiguration) GetCurrentProfile() (*Profile, error) {
	if c.CurrentProfile == "" {
		profiles := c.GetProfiles()

		if len(profiles) > 0 {
			return &profiles[0], nil
		}

		return nil, ErrorProfileNotFound
	}

	return c.GetProfile(c.CurrentProfile), nil
}

// Return the password used for authentication.
func (c *CLIConfiguration) GetPassword() string {
	return c.password
}

func (c *CLIConfiguration) GetToken() string {
	return c.token
}

// Return the URL of the cluster.
func (c *CLIConfiguration) GetUrl() string {
	return c.url
}

// Return the username used for authentication.
func (c *CLIConfiguration) GetUsername() string {
	return c.username
}

// Save the configuration to the file system.
func (c *CLIConfiguration) Save() error {
	ymlData, err := yaml.Marshal(c)

	if err != nil {
		return err
	}

	err = os.WriteFile(c.path, ymlData, 0600)

	if err != nil {
		return err
	}

	return nil
}

func (c *CLIConfiguration) SetAccessKeyId(accessKeyId string) {
	c.accessKeyId = accessKeyId
}

func (c *CLIConfiguration) SetAccessKeySecret(accessKeySecret string) {
	c.accessKeySecret = accessKeySecret
}

func (c *CLIConfiguration) SetInteractive(interactive bool) {
	c.interactive = interactive
}

func (c *CLIConfiguration) SetPassword(password string) {
	c.password = password
}

func (c *CLIConfiguration) SetToken(token string) {
	c.token = token
}

func (c *CLIConfiguration) SetUrl(url string) {
	c.url = strings.TrimRight(url, "/")
}

func (c *CLIConfiguration) SetUsername(username string) {
	c.username = username
}

func (c *CLIConfiguration) SwitchProfile(name string) error {
	profile := c.GetProfile(name)

	if profile == nil {
		return ErrorProfileNotFound
	}

	c.CurrentProfile = name

	return c.Save()
}
