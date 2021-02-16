package icecanekv

import (
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	// KB - Kilobytes
	KB uint64 = 1024

	// MB - Megabytes
	MB uint64 = 1024 * 1024
)

// Peer indicates a single icecanekv instance info
type Peer struct {
	ID      uint64 `yaml:"id"`
	Address string `yaml:"address"`
	Port    string `yaml:"port"`
}

// KVConfig defines the configuration settings for IcecaneKV
type KVConfig struct {
	ID       uint64 `yaml:"id"`
	DbPath   string `yaml:"dbPath"`
	LogLevel string `yaml:"logLevel"`
	Address  string `yaml:"address"`
	Port     string `yaml:"port"`

	// Peers contains the list of all the icecanekv instances including this server. key - ID, value - Peer details.
	Peers map[uint64]Peer `yaml:"peers"`
}

// NewDefaultKVConfig returns a new default key vault configuration.
func NewDefaultKVConfig() *KVConfig {
	return &KVConfig{
		DbPath:   "/tmp/icecane",
		LogLevel: "info",
	}
}

// Validate validates a KVConfig and returns an error if it's invalid.
func (conf *KVConfig) Validate() error {
	if conf.ID == 0 {
		return fmt.Errorf("invalid id provided in config")
	}
	return nil
}

// LoadFromFile loads the config from the file. It assumes that config already has the defaults.
// In the case of an error, it leaves the config untouched.
func (conf *KVConfig) LoadFromFile(path string) {
	log.Info(fmt.Sprintf("icecanekv::config::LoadFromFile; loading config from file %s", path))
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Error(fmt.Sprintf("icecanekv::config::LoadFromFile; error reading config from file %s, error %s", path, err))
		return
	}
	fconf := KVConfig{}
	err = yaml.Unmarshal([]byte(data), &fconf)
	if err != nil {
		log.Error(fmt.Sprintf("icecanekv::config::LoadFromFile; error unmarshalling config from file %s, error %s", path, err))
		return
	}

	log.WithFields(log.Fields{"config": fconf}).Debug("icecanekv::config::LoadFromFile; read contents from the file")

	// populate in the config.
}
