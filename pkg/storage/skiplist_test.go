package storage

import (
	"bytes"
	"fmt"
	"testing"
)

// TestBasicCRUD tests the basic CRUD operations on the skip list
func TestBasicCRUD(t *testing.T) {
	skipList := NewSkipList(10, DefaultComparator)

	skipList.Set([]byte("Key1"), []byte("Value 1"))
	skipList.Set([]byte("Key2"), []byte("Value 2"))
	skipList.Set([]byte("Key3"), []byte("Value 3"))
	skipList.Set([]byte("Key4"), []byte("Value 4"))
	skipList.Set([]byte("Key5"), []byte("Value 5"))
	skipList.Set([]byte("Key6"), []byte("Value 6"))
	skipList.Set([]byte("Key7"), []byte("Value 7"))

	key1Node := skipList.Get([]byte("Key1"))
	if key1Node == nil || bytes.Compare(key1Node.Value(), []byte("Value 1")) != 0 {
		t.Fatal(fmt.Sprintf("Wrong value associated with Key1, Expected %s - Found %s", "Value 1", key1Node.Value()))
	}
}
