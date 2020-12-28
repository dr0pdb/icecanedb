package storage

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
	panic("not implemented")
}
