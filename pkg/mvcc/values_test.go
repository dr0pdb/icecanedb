package mvcc

import (
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestTxnValuesSet(t *testing.T) {
	for i := uint64(0); i < 5; i++ {
		svalue := newSetTxnValue(test.TestValues[i])
		assert.Equal(t, test.TestValues[i], svalue.getUserValue(), "decoded value doesn't match")
		assert.False(t, svalue.getDeleteFlag(), "decoded flag as delete but expected a set")
	}
}

func TestTxnValuesDelete(t *testing.T) {
	for i := uint64(0); i < 5; i++ {
		svalue := newDeleteTxnValue()
		assert.True(t, svalue.getDeleteFlag(), "decoded flag as set but expected a delete")
	}
}
