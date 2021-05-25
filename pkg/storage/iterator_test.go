package storage

import (
	"fmt"
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestKeyValueIterator(t *testing.T) {
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

	// first we set value[0] for each key
	// then we write value[i] for ith key
	// as a result, we have multiple values for the same key in the internal storage.
	for i := range test.TestKeys {
		err = s.Set(test.TestKeys[i], test.TestValues[0], writeOpts)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", i))

		err = s.Set(test.TestKeys[i], test.TestValues[i], writeOpts)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", i))
	}

	itr := s.Scan(test.TestKeys[0])
	expectedCnt := len(test.TestKeys)
	cnt := 0

	for {
		if itr.Valid() {
			assert.Equal(t, test.TestKeys[cnt], itr.Key(), fmt.Sprintf("expected key doesn't match the actual key from iterator at idx: %d", cnt))
			assert.Equal(t, test.TestValues[cnt], itr.Value(), fmt.Sprintf("expected value doesn't match the actual value from iterator at idx: %d", cnt))
			cnt++
			itr.Next()
		} else {
			break
		}
	}

	assert.Equal(t, expectedCnt, cnt, fmt.Sprintf("Number of entries in iterator doesn't match. Expected: %d, actual %d", expectedCnt, cnt))
}

func TestKeyValueIteratorWithSingleKey(t *testing.T) {
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
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", 0))

	err = s.Delete(test.TestKeys[0], writeOpts)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", 0))

	itr := s.Scan(test.TestKeys[0])
	cnt := 0

	for {
		if itr.Valid() {
			cnt++
			itr.Next()
		} else {
			break
		}
	}

	assert.Equal(t, 0, cnt, "expected an empty iterator")
}

func TestKeyValueIteratorWithDeletes(t *testing.T) {
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

	// first we set value[0] for each key
	// then we write value[i] for ith key
	// as a result, we have multiple values for the same key in the internal storage.
	for i := range test.TestKeys {
		err = s.Set(test.TestKeys[i], test.TestValues[i], writeOpts)
		assert.Nil(t, err, fmt.Sprintf("Unexpected error in setting value for key%d", i))

		// if index if odd, we will delete it.
		if i%2 == 1 {
			err = s.Delete(test.TestKeys[i], writeOpts)
			assert.Nil(t, err, fmt.Sprintf("Unexpected error in deleting value for key%d", i))
		}
	}

	itr := s.Scan(test.TestKeys[0])
	expectedCnt := 3 // we only expect index 0,2,4
	cnt := 0

	for {
		if itr.Valid() {
			assert.Equal(t, test.TestKeys[2*cnt], itr.Key(), fmt.Sprintf("expected key doesn't match the actual key from iterator at idx: %d", 2*cnt))
			assert.Equal(t, test.TestValues[2*cnt], itr.Value(), fmt.Sprintf("expected value doesn't match the actual value from iterator at idx: %d", 2*cnt))
			cnt++
			itr.Next()
		} else {
			break
		}
	}

	assert.Equal(t, expectedCnt, cnt, fmt.Sprintf("Number of entries in iterator doesn't match. Expected: %d, actual %d", expectedCnt, cnt))
}
