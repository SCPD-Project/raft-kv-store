package config

import (
	"encoding/json"
	"io/ioutil"
)

const (
	// ConfigFilePath ...
	ConfigFilePath = "config/config.json"
)

// Config to read shards
type Config struct {
	Shards [][]string `json:"shards"`
}

// GetShards reads shard info from config file
func GetShards() (*Config, error) {

	config := &Config{}
	data, err := ioutil.ReadFile(ConfigFilePath)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}
