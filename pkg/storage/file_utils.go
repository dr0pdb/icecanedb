package storage

import (
	"fmt"
	"os"
)

type fileType int

const (
	lockFileType fileType = iota
	dbFileType
)

// getDbFileName returns the name of the file stored on the disk for a particular type and number.
func getDbFileName(dirname, fileName string, fileType fileType) string {
	// reset trailing slashes
	for len(dirname) > 0 && dirname[len(dirname)-1] == os.PathSeparator {
		dirname = dirname[:len(dirname)-1]
	}

	switch fileType {
	case dbFileType:
		return fmt.Sprintf("%s%c%s.db", dirname, os.PathSeparator, fileName)
	case lockFileType:
		return fmt.Sprintf("%s%cLOCK", dirname, os.PathSeparator)
	}

	panic("storage::file_utils::getDbFileName: invalid file type")
}
