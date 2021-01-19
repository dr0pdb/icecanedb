package storage

import (
	"fmt"
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestMemtableGetSetWithSeq(t *testing.T) {
	internalKeyComparator := newInternalKeyComparator(DefaultComparator)
	m := NewMemtable(NewSkipList(10, internalKeyComparator), internalKeyComparator)

	seqNumber := uint64(1)

	// set for key with seq number 1
	ikey1 := newInternalKey(test.TestKeys[0], internalKeyKindSet, seqNumber)
	err := m.Set(ikey1, test.TestValues[0])
	assert.Nil(t, err, "Unexpected error in setting in memtable for ikey1")

	// set new value for same key with seq number 2
	ikey2 := newInternalKey(test.TestKeys[0], internalKeyKindSet, seqNumber+1)
	err = m.Set(ikey2, test.TestValues[1])
	assert.Nil(t, err, "Unexpected error in setting in memtable for ikey2")

	// seq number 2 or above should give latest value which is testValues[1]
	ikey3 := newInternalKey(test.TestKeys[0], internalKeyKindSet, seqNumber+100)
	val, _, err := m.Get(ikey3)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in getting value for key%d", 3))
	assert.Equal(t, test.TestValues[1], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 3, test.TestValues[1], val))

	val, _, err = m.Get(ikey2)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in getting value for key%d", 2))
	assert.Equal(t, test.TestValues[1], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 1, test.TestValues[1], val))

	// seq number 1 should give testValues[0]
	val, _, err = m.Get(ikey1)
	assert.Nil(t, err, fmt.Sprintf("Unexpected error in getting value for key%d", 1))
	assert.Equal(t, test.TestValues[0], val, fmt.Sprintf("Unexpected value for key%d. Expected %v, found %v", 0, test.TestValues[0], val))
}
