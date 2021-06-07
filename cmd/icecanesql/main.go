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

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/dr0pdb/icecanedb/pkg/common"
	"github.com/dr0pdb/icecanedb/pkg/icecanesql"
	log "github.com/sirupsen/logrus"
)

var (
	logFile            = false
	configFilePath     = "/etc/icecanesql.yaml"
	configFilePathFlag = flag.String("configFilePath", "", "overrides the default config file path")
)

func main() {
	flag.Parse()
	log.SetFormatter(&log.JSONFormatter{})

	log.Info("icecanesqlmain::main::main; starting")
	conf := &common.ClientConfig{}
	if *configFilePathFlag != "" {
		configFilePath = *configFilePathFlag
	}
	conf.LoadFromFile(configFilePath)

	if logFile {
		f, err := os.OpenFile("/tmp/logfile-client", os.O_WRONLY|os.O_CREATE, 0755)
		if err == nil {
			log.SetOutput(f)
			defer f.Close()
		}
	}

	var cmd string
	for {
		fmt.Printf("db> ")
		reader := bufio.NewReader(os.Stdin)
		if cmd, _ = reader.ReadString('\n'); true {
			cmd = strings.Trim(cmd, " \n")
		}

		if cmd == "exit" {
			break
		}

		// execute and print result
		c := icecanesql.NewClient("random", conf)
		c.Execute(cmd, icecanesql.NoTxn)
	}
}
