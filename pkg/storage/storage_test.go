package storage

import (
	"bytes"
	"fmt"
	"path"
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

var (
	testKeys   [][]byte = [][]byte{[]byte("Key1"), []byte("Key2"), []byte("Key3"), []byte("Key4"), []byte("Key5")}
	testValues [][]byte = [][]byte{[]byte("Value1"), []byte("Value2"), []byte("Value3"), []byte("Value4"), []byte("Value5")}
)

var testDirectory = path.Join("/tmp", "icecanetest")

// NewTestCustomComparator returns a new instance of storage.Comparator for testing purposes.
func NewTestCustomComparator() Comparator {
	return &customTestComparator{}
}

type customTestComparator struct{}

func (d *customTestComparator) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (d *customTestComparator) Name() string {
	return "CustomTestComparator"
}

func TestOpenDBWithDefaultComparator(t *testing.T) {
	test.CreateTestDirectory(testDirectory)
	defer test.CleanupTestDirectory(testDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}

	s, err := NewStorage(testDirectory, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	assert.Nil(t, err, "Unexpected error in opening database")

	assert.Equal(t, DefaultComparator, s.ukComparator, "Default comparator not set when not passing any custom comparator")
}

func TestOpenDBWithCustomComparator(t *testing.T) {
	test.CreateTestDirectory(testDirectory)
	defer test.CleanupTestDirectory(testDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}

	customComparator := NewTestCustomComparator()

	s, err := NewStorageWithCustomComparator(testDirectory, customComparator, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	assert.Nil(t, err, "Unexpected error in opening database")

	assert.Equal(t, customComparator, s.ukComparator, "Custom comparator is not set properly in the storage")
}

func TestFunctionality(t *testing.T) {
	test.CreateTestDirectory(testDirectory)
	defer test.CleanupTestDirectory(testDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}

	s, err := NewStorage(testDirectory, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	assert.Nil(t, err, "Unexpected error in opening database")

	for i := range testKeys {
		err = s.Set(testKeys[i], testValues[i], nil)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", i))
	}

	for i := range testKeys {
		val, err := s.Get(testKeys[i])
		assert.Nil(t, err)
		assert.Equal(t, testValues[i], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", i, testValues[i], val))
	}

	err = s.Delete(testKeys[0], nil)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 0))

	err = s.Delete(testKeys[1], nil)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 1))

	_, err = s.Get(testKeys[0])
	assert.NotNil(t, err, fmt.Sprintf("Found entry for key%d when it was deleted", 0))

	err = s.Delete(testKeys[1], nil)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 1))

	val, err := s.Get(testKeys[3])
	assert.Nil(t, err)
	assert.Equal(t, testValues[3], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 3, testValues[3], val))
}

// todo
func TestConcurrentFunctionality(t *testing.T) {
	test.CreateTestDirectory(testDirectory)
	defer test.CleanupTestDirectory(testDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}

	s, err := NewStorage(testDirectory, options)
	assert.Nil(t, err)

	err = s.Open()
	assert.Nil(t, err)
}
