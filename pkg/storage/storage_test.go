package storage

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

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
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err, "Unexpected error in opening database")

	assert.Equal(t, DefaultComparator, s.ukComparator, "Default comparator not set when not passing any custom comparator")
}

func TestOpenDBWithCustomComparator(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}

	customComparator := NewTestCustomComparator()

	s, err := NewStorageWithCustomComparator(test.TestDirectory, test.TestDbName, customComparator, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err, "Unexpected error in opening database")

	assert.Equal(t, customComparator, s.ukComparator, "Custom comparator is not set properly in the storage")
}

func TestBasicSingleCRUD(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err, "Unexpected error in opening database")

	err = s.Set(test.TestKeys[0], test.TestValues[0], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", 0))

	val, err := s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in getting value for key%d", 0))
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))

	err = s.Delete(test.TestKeys[0], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 0))

	_, err = s.Get(test.TestKeys[0], nil)
	assert.NotNil(t, err, fmt.Sprintf("Found entry for key%d when it was deleted", 0))
}

func TestBasicMultiplePutReturnsLatestValue(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err, "Unexpected error in opening database")

	oldValue := test.TestValues[0]
	latestValue := test.TestValues[1]

	err = s.Set(test.TestKeys[0], oldValue, writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", 0))

	val, err := s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err)
	assert.Equal(t, oldValue, val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, oldValue, val))

	// update value for key
	err = s.Set(test.TestKeys[0], latestValue, writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", 0))

	// get should return latest value
	val, err = s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err)
	assert.Equal(t, latestValue, val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, latestValue, val))
}

func TestBasicMultipleGetSetDelete(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err, "Unexpected error in opening database")

	for i := range test.TestKeys {
		err = s.Set(test.TestKeys[i], test.TestValues[i], writeOpts)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", i))
	}

	for i := range test.TestKeys {
		val, err := s.Get(test.TestKeys[i], nil)
		assert.Nil(t, err)
		assert.Equal(t, test.TestValues[i], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", i, test.TestValues[i], val))
	}

	err = s.Delete(test.TestKeys[0], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 0))

	err = s.Delete(test.TestKeys[1], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 1))

	_, err = s.Get(test.TestKeys[0], nil)
	assert.NotNil(t, err, fmt.Sprintf("Found entry for key%d when it was deleted", 0))

	err = s.Delete(test.TestKeys[1], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 1))

	val, err := s.Get(test.TestKeys[3], nil)
	assert.Nil(t, err)
	assert.Equal(t, test.TestValues[3], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 3, test.TestValues[3], val))
}

// Spawn go routines to do put, get and delete concurrently.
func TestConcurrentFunctionality(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err)

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err)

	num := 1000
	wg := &sync.WaitGroup{}
	wg.Add(num)

	for i := 0; i < num; i++ {
		go func(idx int, wg *sync.WaitGroup) {
			defer wg.Done()
			tidx := idx % len(test.TestKeys)

			key := []byte(strconv.Itoa(idx))

			err = s.Set(key, test.TestValues[tidx], writeOpts)
			assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key %v", key))

			val, err := s.Get(key, nil)
			assert.Nil(t, err)
			assert.Equal(t, test.TestValues[tidx], val, fmt.Sprintf("Unexpected value for key %v. Expected %v, found %v", key, test.TestValues[tidx], val))

			err = s.Delete(key, writeOpts)
			assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key %v", key))

			_, err = s.Get(key, nil)
			assert.NotNil(t, err, fmt.Sprintf("Found entry for key %v when it was deleted", key))
		}(i, wg)
	}

	wg.Wait()
}

func TestGetLatestSeqNumberForKey(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err)

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err)

	err = s.Set(test.TestKeys[0], test.TestValues[0], writeOpts)
	assert.Nil(t, err)

	sn := s.GetLatestSeqForKey(test.TestKeys[0])

	err = s.Set(test.TestKeys[0], test.TestUpdatedValues[0], writeOpts)
	assert.Nil(t, err)

	sn2 := s.GetLatestSeqForKey(test.TestKeys[0])
	assert.Equal(t, sn+1, sn2)

	err = s.Set(test.TestKeys[1], test.TestUpdatedValues[1], writeOpts)
	assert.Nil(t, err)

	sn3 := s.GetLatestSeqForKey(test.TestKeys[1])
	assert.Equal(t, sn+2, sn3)

	// inserting another key shouldn't change latest seq for this key.
	sn2 = s.GetLatestSeqForKey(test.TestKeys[0])
	assert.Equal(t, sn+1, sn2)
}

func TestStorageScan(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err)

	err = s.Open()
	defer s.Close()

	assert.Nil(t, err)

	for i := range test.TestKeys {
		err = s.Set(test.TestKeys[i], test.TestValues[i], writeOpts)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", i))
	}

	itr := s.Scan(test.TestKeys[2])
	expectedCnt := 3
	cnt := 0

	for {
		if itr.Valid() {
			assert.Equal(t, test.TestKeys[2+cnt], itr.Key(), fmt.Sprintf("expected key doesn't match the actual key from iterator at idx: %d", cnt+2))
			cnt++
			itr.Next()
		} else {
			break
		}
	}

	assert.Equal(t, expectedCnt, cnt, fmt.Sprintf("Number of entries in iterator doesn't match. Expected: %d, actual %d", expectedCnt, cnt))
}

func TestBasicSingleCRUDWithClose(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	options := &Options{
		CreateIfNotExist: true,
	}
	writeOpts := &WriteOptions{
		Sync: true,
	}

	s, err := NewStorage(test.TestDirectory, test.TestDbName, options)
	assert.Nil(t, err, "Unexpected error in creating new storage")

	err = s.Open()
	assert.Nil(t, err, "Unexpected error in opening database")

	err = s.Set(test.TestKeys[0], test.TestValues[0], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", 0))

	val, err := s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in getting value for key%d", 0))
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))

	err = s.Close()
	assert.Nil(t, err, "Unexpected error in closing the database")

	err = s.Open()
	assert.Nil(t, err, "Unexpected error in opening database again after closing")

	val, err = s.Get(test.TestKeys[0], nil)
	assert.Nil(t, err, fmt.Sprintf("After reopening: Unexpected error in getting value for key%d", 0))
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("After reopening: Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))

	err = s.Delete(test.TestKeys[0], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("After reopening: Unexpected error in deleting value for key%d", 0))

	_, err = s.Get(test.TestKeys[0], nil)
	assert.NotNil(t, err, fmt.Sprintf("After reopening: Found entry for key%d when it was deleted", 0))

	err = s.Close()
	assert.Nil(t, err, "Unexpected error in closing the database for the second time")

	err = s.Open()
	assert.Nil(t, err, "Unexpected error in opening database again after closing the second time")

	_, err = s.Get(test.TestKeys[0], nil)
	assert.NotNil(t, err, fmt.Sprintf("After reopening2: Found entry for key%d when it was deleted", 0))
}
