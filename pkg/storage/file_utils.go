package storage

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

type fileType int

const (
	lockFileType fileType = iota
	currentFileType
	manifestFileType
)

// getDbFileName returns the name of the file stored on the disk for a particular type and number.
func getDbFileName(dirname string, fileType fileType, fileNum uint64) string {
	log.WithFields(log.Fields{
		"dirname":  dirname,
		"fileType": fileType,
		"fileNum":  fileNum,
	}).Info("storage::file_utils: getDbFileName")

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
		return fmt.Sprintf("%s%cMANIFEST-%06d", dirname, os.PathSeparator, fileNum)
	}

	panic("invalid file type")
}

// setCurrentFile creates a current file and saves the name of the manifest file inside it.
//
// It first creates a .dbtmp file, writes the name of manifest file and then renames it to CURRENT.
func setCurrentFile(dirname string, fs FileSystem, manifestFileNumber uint64) error {
	log.WithFields(log.Fields{
		"dirname":            dirname,
		"manifestFileNumber": manifestFileNumber,
	}).Info("storage::file_utils: setCurrentFile")

	currentFileName := getDbFileName(dirname, currentFileType, manifestFileNumber)
	tmpFileName := fmt.Sprintf("%s.%06d.dbtmp", currentFileName, manifestFileNumber)
	fs.remove(tmpFileName)
	f, err := fs.create(tmpFileName)
	if err != nil {
		log.Error("storage::file_utils: setCurrentFile; failure in creating tmp file.")
		return err
	}

	if _, err := fmt.Fprintf(f, "MANIFEST-%06d\n", manifestFileNumber); err != nil {
		log.Error("storage::file_utils: setCurrentFile; failure in writing to tmp file.")
		return err
	}

	if err := f.Close(); err != nil {
		log.Error("storage::file_utils: setCurrentFile; failure in closing tmp file.")
		return err
	}

	if err := fs.rename(tmpFileName, currentFileName); err != nil {
		log.Error("storage::file_utils: setCurrentFile; failure in renaming tmp to current.")
		return err
	}

	log.Info("storage::file_utils: setCurrentFile; successfully set the current file with content.")
	return nil
}
