package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/dr0pdb/icecanedb/pkg/storage"
	"github.com/dr0pdb/icecanedb/test"
)

var (
	key1   = []byte("Key1")
	key2   = []byte("Key2")
	key3   = []byte("Key3")
	key4   = []byte("Key4")
	key5   = []byte("Key5")
	value1 = []byte("Value 1")
	value2 = []byte("Value 2")
	value3 = []byte("Value 3")
	value4 = []byte("Value 4")
	value5 = []byte("Value 5")
)

func main() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(dir)

	opts := &storage.Options{
		CreateIfNotExist: true,
	}
	s, err := storage.NewStorage(path.Join(dir, "../../_output/"), test.TestDbName, opts)
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	err = s.Open()
	if err != nil {
		fmt.Print(err.Error())
		return
	}

	wopts := &storage.WriteOptions{
		Sync: true,
	}
	s.Set(key1, value1, wopts)
	s.Set(key2, value2, wopts)
	s.Set(key3, value3, wopts)
	r1, err := s.Get(key1, nil)
	fmt.Printf("value for key %v is %v", key1, r1)
	r2, err := s.Get(key2, nil)
	fmt.Printf("value for key %v is %v", key2, r2)
	r3, err := s.Get(key3, nil)
	fmt.Printf("value for key %v is %v", key3, r3)
	err = s.Delete(key1, wopts)
	r1, err = s.Get(key1, nil)
	fmt.Printf("value for key %v after deletion is %v", key1, r1)
}
