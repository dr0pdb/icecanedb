package storage

import (
	"encoding/binary"

	log "github.com/sirupsen/logrus"
)

// header has 8 bytes of sequence number and 4 bytes for the count of records.
const batchHeaderSize = 12

// writeBatch contains a number of Put/Delete records written atomically.
// Refer to https://github.com/google/leveldb/blob/master/db/write_batch.cc for format.
type writeBatch struct {
	data []byte
}

// init initializes a write batch with size headerSize and capacity cap rounded to nearest power of 2.
func (wb *writeBatch) init(cap int) {
	icap := 256
	for icap < cap {
		icap *= 2
	}
	wb.data = make([]byte, batchHeaderSize, icap)
}

func (wb *writeBatch) set(key, value []byte) {
	log.WithFields(log.Fields{"key": key, "value": value}).Info("storage::write_batch: set; start")
	if len(wb.data) == 0 {
		wb.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderSize)
	}

	if wb.incrementCount() {
		wb.data = append(wb.data, byte(internalKeyKindSet))
		wb.appendStr(key)
		wb.appendStr(value)
	} else {
		log.Error("storage::write_batch: set; error in incrementing count")
	}

	log.Info("storage::write_batch: set; done")
}

func (wb *writeBatch) delete(key []byte) {
	log.WithFields(log.Fields{"key": key}).Info("storage::write_batch: delete; start")

	if len(wb.data) == 0 {
		wb.init(len(key) + binary.MaxVarintLen64 + batchHeaderSize)
	}

	if wb.incrementCount() {
		wb.data = append(wb.data, byte(internalKeyKindDelete))
		wb.appendStr(key)
	}

	log.Info("storage::write_batch: delete; done")
}

func (wb *writeBatch) getSeqNumData() []byte {
	return wb.data[:8]
}

func (wb *writeBatch) getCountData() []byte {
	return wb.data[8:12]
}

func (wb *writeBatch) incrementCount() bool {
	d := wb.getCountData()
	for i := range d {
		d[i]++
		if d[i] != 0x00 {
			return true
		}
	}

	// invalid
	d[0] = 0xff
	d[1] = 0xff
	d[2] = 0xff
	d[3] = 0xff

	return false
}

func (wb *writeBatch) appendStr(s []byte) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(s)))
	wb.data = append(wb.data, buf[:n]...)
	wb.data = append(wb.data, s...)
}

func (wb *writeBatch) setSeqNum(seqNum uint64) {
	binary.LittleEndian.PutUint64(wb.getSeqNumData(), seqNum)
}

func (wb *writeBatch) getSeqNum() uint64 {
	return binary.LittleEndian.Uint64(wb.getSeqNumData())
}

func (wb *writeBatch) getCount() uint32 {
	return binary.LittleEndian.Uint32(wb.getCountData())
}
