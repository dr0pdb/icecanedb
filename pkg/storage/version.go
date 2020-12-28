package storage

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

// fileMetaData stores the meta data about a file.
type fileMetaData struct {
	fileNum uint64

	size uint64

	smallest, largest internalKey
}

// custom comparator to sort files by file number
type byFileNum []fileMetaData

func (b byFileNum) Len() int           { return len(b) }
func (b byFileNum) Less(i, j int) bool { return b[i].fileNum < b[j].fileNum }
func (b byFileNum) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// custom comparator to sort files by file number
type bySmallest struct {
	data         []fileMetaData
	ikComparator Comparator
}

func (b bySmallest) Len() int { return len(b.data) }
func (b bySmallest) Less(i, j int) bool {
	return b.ikComparator.Compare(b.data[i].smallest, b.data[j].smallest) < 0
}
func (b bySmallest) Swap(i, j int) { b.data[i], b.data[j] = b.data[j], b.data[i] }

type version struct {
	// files contains the levelwise file information
	files [defaultNumberLevels][]fileMetaData

	prev, next *version
}

// checkFiles checks if the version is consistent with the sorting rules.
// for level 0, files should be sorted by file number
// for level 1 and above, files should be sorted internal key and should be non-overlapping.
func (v *version) checkFiles(ikComparator Comparator) error {
	log.Info("storage::version: checkFiles; started")
	for level, files := range v.files {
		if level == 0 {
			prevFileNumber := uint64(0)

			for i, f := range files {
				if i > 0 && prevFileNumber >= f.fileNum {
					log.Error("storage::version: checkFiles; level 0 files in the version are not sorted by file numbers")
					return fmt.Errorf("icecanedb: level 0 files in the version are not sorted by file numbers")
				}
				prevFileNumber = f.fileNum
			}
		} else {
			prevLargest := internalKey(nil)
			for i, f := range files {
				if i > 0 && ikComparator.Compare(prevLargest, f.smallest) >= 0 {
					log.Error("storage::version: checkFiles; level 1 or above files in the version are not disjoint by internal key")
					return fmt.Errorf("icecanedb: level 1 or above files in the version are not disjoint by internal key")
				}
				if ikComparator.Compare(f.smallest, f.largest) > 0 {
					log.Error("storage::version: checkFiles; level 1 or above files in the version are not sorted by internal key")
					return fmt.Errorf("icecanedb: level 1 or above files in the version are not sorted by internal key")
				}
				prevLargest = f.largest
			}
		}
	}
	log.Info("storage::version: checkFiles; ended")
	return nil
}
