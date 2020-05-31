package config

import (
	"encoding/json"
	"io/ioutil"
)

const (
	// ShardConfigFilePath is the file path of shard configuration
	ShardConfigFilePath = "config/shard-config.json"
)

// ShardsConfig to read shards json file
type ShardsConfig struct {
	Shards [][]string `json:"shards"`
}

// GetShards reads shard info from config file
func GetShards() (*ShardsConfig, error) {
	config := &ShardsConfig{}
	data, err := ioutil.ReadFile(ShardConfigFilePath)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}
