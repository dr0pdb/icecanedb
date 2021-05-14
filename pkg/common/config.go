/**
 * Copyright 2020 The IcecaneDB Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

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
	ID      uint64 `yaml:"id"`
	DbPath  string `yaml:"dbPath"`
	Address string `yaml:"address"`
	Port    string `yaml:"port"`

	// Peers contains the list of all the icecanekv instances excluding this server.
	Peers []Peer `yaml:"peers"`

	// Logging config
	LogMVCC    bool
	LogRaft    bool
	LogStorage bool
}

// NewDefaultKVConfig returns a new default key vault configuration.
func NewDefaultKVConfig() *KVConfig {
	return &KVConfig{
		DbPath: "/var/lib/icecanekv",
	}
}

// Validate validates a KVConfig and returns an error if it's invalid.
func (conf *KVConfig) Validate() error {
	if conf.ID == 0 {
		return fmt.Errorf("invalid id provided in config")
	}
	if conf.DbPath == "" {
		return fmt.Errorf("invalid db path provided in config")
	}
	if conf.Address == "" {
		return fmt.Errorf("invalid address provided in config")
	}
	if conf.Port == "" {
		return fmt.Errorf("invalid port provided in config")
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

	// populate fields
	if fconf.ID != 0 {
		conf.ID = fconf.ID
	}
	if fconf.DbPath != "" {
		conf.DbPath = fconf.DbPath
	}
	if fconf.Address != "" {
		conf.Address = fconf.Address
	}
	if fconf.Port != "" {
		conf.Port = fconf.Port
	}
	if len(fconf.Peers) != 0 {
		conf.Peers = fconf.Peers
	}
}

// ClientConfig defines the configuration settings for IcecaneSQL
type ClientConfig struct {
	Servers []Peer `yaml:"servers"`
}

// LoadFromFile reads the client config from the file
func (conf *ClientConfig) LoadFromFile(path string) {
	log.Info(fmt.Sprintf("icecanesql::config::LoadFromFile; loading config from file %s", path))
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Error(fmt.Sprintf("icecanesql::config::LoadFromFile; error reading config from file %s, error %s", path, err))
		return
	}
	fconf := KVConfig{}
	err = yaml.Unmarshal([]byte(data), &fconf)
	if err != nil {
		log.Error(fmt.Sprintf("icecanesql::config::LoadFromFile; error unmarshalling config from file %s, error %s", path, err))
		return
	}

	log.WithFields(log.Fields{"config": fconf}).Debug("icecanesql::config::LoadFromFile; read contents from the file")
}
