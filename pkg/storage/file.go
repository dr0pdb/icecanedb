package storage

import (
	"io"
	"os"
)

// file is an file abstraction.
//
// It can be *os.File or an in-memory file.
type file interface {
	io.Reader
	io.Writer
}

// fileSystem is the file system abstraction.
//
// Contains functions which can be used to interact with the file system.
type fileSystem interface {
	create(name string) (file, error)

	open(name string) (file, error)

	remove(name string) error

	rename(oldname, newname string) error

	mkdirAll(dir string, perm os.FileMode) error
}
