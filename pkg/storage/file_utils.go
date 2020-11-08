package storage

import (
	"fmt"
	"os"
)

type fileType int

const (
	lockFileType fileType = iota
	currentFileType
	manifestFileType
)

// getDbFileName returns the name of the file stored on the disk for a particular type and number.
func getDbFileName(dirname string, fileType fileType, fileNum int64) string {
	// reset trailing slashes
	for len(dirname) > 0 && dirname[len(dirname)-1] == os.PathSeparator {
		dirname = dirname[:len(dirname)-1]
	}

	switch fileType {
	case lockFileType:
		return fmt.Sprintf("%s%c%06d.log", dirname, os.PathSeparator, fileNum)
	case currentFileType:
		return fmt.Sprintf("%s%cCURRENT", dirname, os.PathSeparator)
	case manifestFileType:
		return fmt.Sprintf("%s%cMANIFEST", dirname, os.PathSeparator)
	}

	panic("invalid file type")
}
