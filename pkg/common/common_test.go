package common

import (
	"fmt"
	"testing"

	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestUint64BytesConversion(t *testing.T) {
	test.CreateTestDirectory(test.TestDirectory)
	defer test.CleanupTestDirectory(test.TestDirectory)

	n := uint64(121)
	b := U64ToByte(n)
	assert.Equal(t, n, ByteToU64(b), fmt.Sprintf("Unexpected error in uint64-bytes comparison; expected %d actual %d", n, ByteToU64(b)))
}
