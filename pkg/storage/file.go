package storage

import (
	"io"
	"os"
)

// file is an file abstraction.
//
// It can be *os.File or an in-memory file.
type file interface {
	io.Closer
	io.Reader
	io.Writer
	Stat() (os.FileInfo, error) // https://golang.org/pkg/os/#File.Stat
	Sync() error
}

// FileSystem is the file system abstraction.
//
// Contains functions which can be used to interact with the file system.
// Mainly a 1:1 mapping over the File interface: https://golang.org/pkg/os/#File
type FileSystem interface {
	// create creates or truncates the file.
	create(name string) (file, error)

	// open opens the file for reading.
	// returns error if the file is not found.
	open(name string) (file, error)

	// openFile opens the file with the given flags and mode.
	// returns error if the file is not found.
	openFile(name string, flag int, perm os.FileMode) (file, error)

	// remove removes the file.
	// returns error if the file isn't found.
	remove(name string) error

	// rename renames the file from oldname to newname.
	// return error if the file with oldname is not found.
	rename(oldname, newname string) error

	// mkdirAll creates a dir with all the parents.
	//
	// returns nil if the operation was success or the dir already exists.
	mkdirAll(dir string, perm os.FileMode) error

	// lock creates a lock file in the directory.
	//
	// this is used to obtain exclusive access to the directory.
	lock(name string) error

	// stat returns the FileInfo structure describing file. If there is an error, it will be of type *PathError
	stat(name string) (os.FileInfo, error)
}

// DefaultFileSystem is a FileSystem implementation of the operating system.
var DefaultFileSystem FileSystem = defaultFileSystem{}

type defaultFileSystem struct{}

// create creates or truncates the file.
func (dfs defaultFileSystem) create(name string) (file, error) {
	return os.Create(name)
}

// open opens the file for reading.
// returns error if the file is not found.
func (dfs defaultFileSystem) open(name string) (file, error) {
	return os.Open(name)
}

// openFile opens the file with the given flags and mode.
// returns error if the file is not found.
func (dfs defaultFileSystem) openFile(name string, flag int, perm os.FileMode) (file, error) {
	return os.OpenFile(name, flag, perm)
}

// remove removes the file.
// returns error if the file isn't found.
func (dfs defaultFileSystem) remove(name string) error {
	return os.Remove(name)
}

// rename renames the file from oldname to newname.
// return error if the file with oldname is not found.
func (dfs defaultFileSystem) rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

// mkdirAll creates a dir with all the parents.
//
// returns nil if the operation was success or the dir already exists.
func (dfs defaultFileSystem) mkdirAll(dir string, perm os.FileMode) error {
	return os.MkdirAll(dir, perm)
}

// lock creates a lock file in the directory.
//
// this is used to obtain exclusive access to the directory.
func (dfs defaultFileSystem) lock(dir string) error {
	_, err := dfs.create(dir)
	return err
}

// stat returns the FileInfo structure describing file. If there is an error, it will be of type *PathError
func (dfs defaultFileSystem) stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}
