package main

import (
	"flag"

	"github.com/dr0pdb/icecanedb/pkg/icecanekv"

	log "github.com/sirupsen/logrus"
)

var (
	dbPath   = flag.String("path", "", "directory path of the db")
	logLevel = flag.String("loglevel", "", "the level of log")
)

func main() {
	flag.Parse()
	conf := icecanekv.NewDefaultKVConfig()

	// TODO: update default config according to flags

	err := conf.Validate()
	if err != nil {
		log.Fatalf("%V", err)
	}

	_, err = icecanekv.NewKVServer(conf)
	if err != nil {
		log.Fatalf("%V", err)
	}

}
