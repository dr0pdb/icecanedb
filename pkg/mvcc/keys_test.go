package mvcc

import (
	"bytes"
	"os"
	"testing"

	"github.com/dr0pdb/icecanedb/pkg/storage"
	"github.com/dr0pdb/icecanedb/test"
	"github.com/stretchr/testify/assert"
)

func TestTxnKeyEncodingDecoding(t *testing.T) {
	for i := uint64(1); i <= 1000; i++ {
		txnID := uint64(100)
		txnKey := newTxnKey(test.TestKeys[i%5], txnID)

		assert.Equal(t, test.TestKeys[i%5], txnKey.userKey(), "decoded user key doesn't match")
		assert.Equal(t, txnID, txnKey.txnID(), "decoded txnId doesn't match")
	}
}

// harness to test the txn key comparator and scan operations behaviour
type txnKeyComparatorTestHarness struct {
	s *storage.Storage
}

func newTxnKeyComparatorTestHarness() *txnKeyComparatorTestHarness {
	txnComp := NewtxnKeyComparator()
	os.MkdirAll(testDirectory, os.ModePerm)
	s, _ := storage.NewStorageWithCustomComparator(testDirectory, "testdb", txnComp, &storage.Options{CreateIfNotExist: true})
	s.Open()

	return &txnKeyComparatorTestHarness{
		s: s,
	}
}

func (h *txnKeyComparatorTestHarness) cleanup() {
	h.s.Close()
	os.RemoveAll(testDirectory)
}

// A single txn should get the writes for the increasing keys in the same order
func TestTxnKeyComparatorScanSingleTxn(t *testing.T) {
	h := newTxnKeyComparatorTestHarness()
	defer h.cleanup()

	txnID := uint64(99)
	for i := 0; i < 5; i++ {
		txnKey := newTxnKey(test.TestKeys[i], txnID)
		err := h.s.Set(txnKey, test.TestValues[i], nil)
		assert.Nil(t, err, "Unexpected error during set request")
	}

	startTxnKey := newTxnKey(test.TestKeys[0], txnID)
	itr := h.s.Scan(startTxnKey)
	idx := uint64(0)

	for {
		if itr.Valid() {
			txnKey := TxnKey(itr.Key())
			assert.Equal(t, test.TestKeys[idx], txnKey.userKey(), "user key from decoded key doesn't match the original")
			assert.Equal(t, txnID, txnKey.txnID(), "txn id from decoded key doesn't match with original")
			assert.Equal(t, test.TestValues[idx], itr.Value(), "value from decoded key doesn't match with original")

			itr.Next()
			idx++
		} else {
			break
		}
	}

	assert.Equal(t, uint64(5), idx, "expected a total of 5 kv pairs")
}

// Multiple txns writing to the same key should be sorted in decreasing order of txn id
func TestTxnKeyComparatorScanSingleKey(t *testing.T) {
	h := newTxnKeyComparatorTestHarness()
	defer h.cleanup()

	for i := uint64(1); i <= 100; i++ {
		txnKey := newTxnKey(test.TestKeys[0], i)
		err := h.s.Set(txnKey, test.TestValues[0], nil)
		assert.Nil(t, err, "Unexpected error during set request")
	}

	startTxnKey := newTxnKey(test.TestKeys[0], uint64(100))
	itr := h.s.Scan(startTxnKey)
	idx := uint64(100) // should be in decreasing order to txn id

	for {
		if itr.Valid() {
			txnKey := TxnKey(itr.Key())
			assert.Equal(t, test.TestKeys[0], txnKey.userKey(), "user key from decoded key doesn't match the original")
			assert.Equal(t, idx, txnKey.txnID(), "txn id from decoded key doesn't match with original")
			assert.Equal(t, test.TestValues[0], itr.Value(), "value from decoded key doesn't match with original")

			itr.Next()
			idx--
		} else {
			break
		}
	}

	assert.Equal(t, uint64(0), idx, "expected a total of 100 kv pairs")
}

// Multiple txns writing to the multiple keys
// Keys should be first sorted by user keys in increasing order
// Same keys should be sorted in decreasing order according to txn id
func TestTxnKeyComparatorScanMultipleKeyMultipleTxns(t *testing.T) {
	h := newTxnKeyComparatorTestHarness()
	defer h.cleanup()

	for txnID := uint64(1); txnID <= 100; txnID++ {
		for keyId := uint64(0); keyId < 5; keyId++ {
			txnKey := newTxnKey(test.TestKeys[keyId], txnID)
			err := h.s.Set(txnKey, test.TestValues[keyId], nil)
			assert.Nil(t, err, "Unexpected error during set request")
		}
	}

	for keyId := uint64(0); keyId < 5; keyId++ {
		startTxnKey := newTxnKey(test.TestKeys[keyId], uint64(100))
		itr := h.s.Scan(startTxnKey)
		idx := uint64(100) // should be in decreasing order to txn id

		for {
			if itr.Valid() {
				txnKey := TxnKey(itr.Key())
				if !bytes.Equal(txnKey.userKey(), test.TestKeys[keyId]) {
					break
				}

				assert.Equal(t, test.TestKeys[keyId], txnKey.userKey(), "user key from decoded key doesn't match the original")
				assert.Equal(t, idx, txnKey.txnID(), "txn id from decoded key doesn't match with original")
				assert.Equal(t, test.TestValues[keyId], itr.Value(), "value from decoded key doesn't match with original")

				itr.Next()
				idx--
			} else {
				break
			}
		}

		assert.Equal(t, uint64(0), idx, "expected a total of 100 kv pairs for the single user key")
	}
}
