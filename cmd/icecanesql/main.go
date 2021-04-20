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
		c := icecanesql.NewClient("random")
		c.Execute(cmd)
	}
}
