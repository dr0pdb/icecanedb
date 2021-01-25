package storage

import (
	"encoding/binary"

	log "github.com/sirupsen/logrus"
)

// header has 8 bytes of sequence number and 4 bytes for the count of records.
const batchHeaderSize = 12

// WriteBatch contains a number of Put/Delete records written atomically.
// Refer to https://github.com/google/leveldb/blob/master/db/write_batch.cc for format.
type WriteBatch struct {
	data []byte
}

// init initializes a write batch with size headerSize and capacity cap rounded to nearest power of 2.
func (wb *WriteBatch) init(cap int) {
	icap := 256
	for icap < cap {
		icap *= 2
	}
	wb.data = make([]byte, batchHeaderSize, icap)
}

// Set adds a value for the given key in the write batch.
func (wb *WriteBatch) Set(key, value []byte) {
	log.WithFields(log.Fields{"key": string(key), "value": string(value)}).Info("storage::write_batch: Set; start")
	if len(wb.data) == 0 {
		wb.init(len(key) + len(value) + 2*binary.MaxVarintLen64 + batchHeaderSize)
	}

	if wb.incrementCount() {
		wb.data = append(wb.data, byte(internalKeyKindSet))
		wb.appendStr(key)
		wb.appendStr(value)
	} else {
		log.Error("storage::write_batch: Set; error in incrementing count")
	}

	log.Info("storage::write_batch: Set; done")
}

// Delete adds a delete entry for the given key in the write batch.
func (wb *WriteBatch) Delete(key []byte) {
	log.WithFields(log.Fields{"key": string(key)}).Info("storage::write_batch: Delete; start")

	if len(wb.data) == 0 {
		wb.init(len(key) + binary.MaxVarintLen64 + batchHeaderSize)
	}

	if wb.incrementCount() {
		wb.data = append(wb.data, byte(internalKeyKindDelete))
		wb.appendStr(key)
	}

	log.Info("storage::write_batch: Delete; done")
}

func (wb *WriteBatch) getSeqNumData() []byte {
	return wb.data[:8]
}

func (wb *WriteBatch) getCountData() []byte {
	return wb.data[8:12]
}

func (wb *WriteBatch) incrementCount() bool {
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

func (wb *WriteBatch) appendStr(s []byte) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(s)))
	wb.data = append(wb.data, buf[:n]...)
	wb.data = append(wb.data, s...)
}

func (wb *WriteBatch) setSeqNum(seqNum uint64) {
	binary.LittleEndian.PutUint64(wb.getSeqNumData(), seqNum)
}

func (wb *WriteBatch) getSeqNum() uint64 {
	return binary.LittleEndian.Uint64(wb.getSeqNumData())
}

func (wb *WriteBatch) getCount() uint32 {
	return binary.LittleEndian.Uint32(wb.getCountData())
}

func (wb *WriteBatch) getIterator() batchIterator {
	return wb.data[batchHeaderSize:]
}

type batchIterator []byte

func (bi *batchIterator) next() (kind internalKeyKind, ukey []byte, value []byte, ok bool) {
	log.Info("storage::write_batch: next; started")
	tmp := *bi
	if len(tmp) == 0 {
		log.Error("storage::write_batch: next; next called on an empty batch iterator")
		return 0, nil, nil, false
	}

	kind, *bi = internalKeyKind(tmp[0]), tmp[1:]
	// todo: validate if kind is valid

	ukey, ok = bi.nextString()
	if !ok {
		log.Error("storage::write_batch: next; ukey for internal key set not found.")
		return 0, nil, nil, ok
	}

	if kind != internalKeyKindDelete {
		value, ok = bi.nextString()
		if !ok {
			log.Error("storage::write_batch: next; value for internal key set not found.")
			return 0, nil, nil, ok
		}
	}

	log.Info("storage::write_batch: next; done")
	return kind, ukey, value, true
}

// nextString gets the next string from the batch.
// it reads the length of the string stored as varint and then reads the actual string
func (bi *batchIterator) nextString() (s []byte, ok bool) {
	log.Info("storage::write_batch: nextString; started")
	tmp := *bi

	// u is the length of the string.
	u, numBytes := binary.Uvarint(tmp)
	if numBytes <= 0 {
		log.Error("storage::write_batch: nextString; corrupt value of length of the string.")
		return nil, false
	}

	tmp = tmp[numBytes:]
	if u > uint64(len(tmp)) {
		log.Error("storage::write_batch: nextString; corrupt value of length of string. u is greater than the length of the buffer.")
		return nil, false
	}

	s, *bi = tmp[:u], tmp[u:]
	log.Info("storage::write_batch: nextString; done")
	return s, true
}
