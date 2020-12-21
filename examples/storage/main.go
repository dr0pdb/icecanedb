package main

import (
	"fmt"

	"github.com/dr0pdb/icecanedb/pkg/storage"
)

func main() {
	opts := &storage.Options{
		CreateIfNotExist: true,
	}
	s, err := storage.NewStorage("./example-directory", opts)
	if err != nil {
		fmt.Errorf(err.Error())
	}
	s.Open()
}
