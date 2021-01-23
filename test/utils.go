package test

import (
	"io/ioutil"
	"os"
	"path"
)

var (
	// TestKeys - test data
	TestKeys [][]byte = [][]byte{[]byte("Key1"), []byte("Key2"), []byte("Key3"), []byte("Key4"), []byte("Key5")}

	// TestValues - test data
	TestValues [][]byte = [][]byte{[]byte("Value1"), []byte("Value2"), []byte("Value3"), []byte("Value4"), []byte("Value5")}

	// TestUpdatedValues - updated test data
	TestUpdatedValues [][]byte = [][]byte{[]byte("UpdatedValue1"), []byte("UpdatedValue2"), []byte("UpdatedValue3"), []byte("UpdatedValue4"), []byte("UpdatedValue5")}
)

// CreateTestDirectory creates a test directory for running tests.
func CreateTestDirectory(testDirectory string) {
	os.MkdirAll(testDirectory, os.ModePerm)
}

// CleanupTestDirectory cleans up the test directory.
func CleanupTestDirectory(testDirectory string) error {
	dir, err := ioutil.ReadDir(testDirectory)
	if err != nil {
		return err
	}
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{testDirectory, d.Name()}...))
	}
	return nil
}
