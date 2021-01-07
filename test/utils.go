package test

import (
	"io/ioutil"
	"os"
	"path"
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
