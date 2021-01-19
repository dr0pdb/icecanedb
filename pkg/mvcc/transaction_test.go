package mvcc

import "testing"

var (
	mvcc *MVCC = newTestMVCC()
)

func newTestTransaction(id uint64) *Transaction {
	return &Transaction{
		id:   id,
		mvcc: mvcc,
	}
}

func TestBasicGetSetDelete(t *testing.T) {

}

func TestConcurrentGetSetDelete(t *testing.T) {

}
