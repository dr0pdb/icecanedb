package storage

import (
	"sync"
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

	skipList.set(key1, value1)
	skipList.set(key2, value2)
	skipList.set(key3, value3)

	key1Node := skipList.get(key1)
	assert.Equal(t, value1, key1Node.getValue(), "Value for Key1 is different than what's set in Skiplist.")

	skipList.set(key4, value4)
	skipList.set(key5, value5)

	key2Node := skipList.get(key2)
	assert.Equal(t, value2, key2Node.getValue(), "Value for Key2 is different than what's set in Skiplist.")

	key5Node := skipList.get(key5)
	assert.Equal(t, value5, key5Node.getValue(), "Value for Key5 is different than what's set in Skiplist.")

	key2Node = skipList.delete(key2)
	assert.NotNil(t, key2Node)
	assert.Nil(t, skipList.get(key2))

	key2Node = skipList.set(key2, value2)
	assert.Equal(t, value2, key2Node.getValue(), "Value for Key2 is different than what's set in Skiplist.")
}

// TestBasicCRUD tests the concurrency operations on the skip list
func TestConcurrency(t *testing.T) {
	skipList := newSkipList(10, DefaultComparator)
	l := 100000

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < l; i++ {
			skipList.set([]byte(string(i)), []byte(string(i)))
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < l; i++ {
			skipList.set([]byte(string(i+l)), []byte(string(i+l)))
		}
		wg.Done()
	}()

	wg.Wait()

	for i := 0; i < l; i++ {
		node1 := skipList.get([]byte(string(i)))
		node2 := skipList.get([]byte(string(i + l)))
		assert.NotNil(t, node1)
		assert.NotNil(t, node2)
		assert.Equal(t, []byte(string(i)), node1.getValue(), "Value mismatch in concurrency testing.")
		assert.Equal(t, []byte(string(i+l)), node2.getValue(), "Value mismatch in concurrency testing.")
	}
}

func TestSkipListIterator(t *testing.T) {
	skipList := newSkipList(10, DefaultComparator)

	l := 100
	for i := 0; i < l; i++ {
		skipList.set([]byte(string(i)), []byte(string(i)))
	}

	itr := skipList.newSkipListIterator()
	assert.Equal(t, false, itr.Valid(), "New iterator was expected to be invalid.")
	itr.SeekToFirst()
	assert.Equal(t, true, itr.Valid(), "New iterator after seek to first should be valid.")
	assert.Equal(t, []byte(string(0)), itr.Value(), "First value should be 0.")

	itr.Seek([]byte(string(10)))
	assert.Equal(t, []byte(string(10)), itr.Value(), "Seek to 10 should move the iterator to 10th value.")

	itr.SeekToFirst()
	assert.Equal(t, true, itr.Valid(), "Seek to first should first in backward direction as well.")
}
