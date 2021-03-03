package mvcc

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

type keyType int

const (
	nxtTxnID keyType = iota
)

const (
	notUsed = 0
)

// getKey returns key for the given mvcc key type
func getKey(txnID uint64, keyType keyType) string {
	log.Info("mvcc::key::getKey; start")

	switch keyType {
	case nxtTxnID:
		return fmt.Sprintf("nxtTxnID")
	}

	panic("invalid key type")
}
