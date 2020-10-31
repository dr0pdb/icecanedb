package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

// TestBasicCRUD tests the basic CRUD operations on the skip list
func TestBasicCRUD(t *testing.T) {
	skipList := newSkipList(10, DefaultComparator)

	skipList.Set(key1, value1)
	skipList.Set(key2, value2)
	skipList.Set(key3, value3)

	key1Node := skipList.Get(key1)
	assert.Equal(t, value1, key1Node.Value(), "Value for Key1 is different than what's set in Skiplist.")

	skipList.Set(key4, value4)
	skipList.Set(key5, value5)

	key2Node := skipList.Get(key2)
	assert.Equal(t, value2, key2Node.Value(), "Value for Key2 is different than what's set in Skiplist.")

	key5Node := skipList.Get(key5)
	assert.Equal(t, value5, key5Node.Value(), "Value for Key5 is different than what's set in Skiplist.")

	key2Node = skipList.Delete(key2)
	assert.NotNil(t, key2Node)
	assert.Nil(t, skipList.Get(key2))

	key2Node = skipList.Set(key2, value2)
	assert.Equal(t, value2, key2Node.Value(), "Value for Key2 is different than what's set in Skiplist.")
}
