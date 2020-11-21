package storage

import (
	"io"

	log "github.com/sirupsen/logrus"
)

// The log record format details can be found at the below link.
// https://github.com/google/leveldb/blob/master/doc/log_format.md
//
//

const (
	blockSize  = 32 * 1024
	headerSize = 7
)

type logRecordWriter struct {
	// w is the writer that logRecordWriter writes to
	w io.Writer

	// seq is the sequence number of the current record.
	seq int

	// buffer
	buf [blockSize]byte

	// buf[lo:hi] is the current chunk position including the header
	lo, hi int

	// buf[:sofar] has been written to w. can be stale if flush hasn't been called.
	sofar int

	// baseOffset is the offset in w at which the writing of log record started.
	baseOffset int64

	// blockNumber is the block that is currently stored in buf
	blockNumber int64

	lastRecordOffset int64

	// pending indicates if there is a chunk that is yet to written but is buffered.
	pending bool

	// first indicates if the current chunk is the first chunk of the record.
	first bool

	// err is any error encountered during any log record writer operation.
	err error
}

func (lrw *logRecordWriter) writeBlock() {
	panic("Not Implemented")
}

// newLogRecordWriter creates a new log record writer.
func newLogRecordWriter(w io.Writer) *logRecordWriter {
	var offset int64
	if s, ok := w.(io.Seeker); ok {
		var err error
		if offset, err = s.Seek(0, io.SeekCurrent); err != nil {
			offset = 0
		}
	}

	return &logRecordWriter{
		w:                w,
		baseOffset:       offset,
		lastRecordOffset: -1,
	}
}

// next returns a io.Writer for the next record.
func (lrw *logRecordWriter) next() (io.Writer, error) {
	lrw.seq++
	if lrw.err != nil {
		log.WithFields(log.Fields{"error": lrw.err.Error()}).Error("storage::logrecord: next; existing background error found in the log record writer.")
		return nil, lrw.err
	}

	log.WithFields(log.Fields{"logRecordWriter": lrw}).Info("storage::logrecord: next; next called on the log record writer.")

	if lrw.pending {
		log.Info("storage::logrecord: next; found pending chunk.")
		// todo: fill the header of this pending chunk.
	}

	// move pointers for the next chunk headers
	lrw.lo = lrw.hi
	lrw.hi = lrw.hi + headerSize

	// check if there is enough size to fit in at least the header and one byte of data.
	// check the link at the start for more.
	if lrw.hi > blockSize {
		log.Info("storage::logrecord: next; not enough space in current block.")

		// fill the rest with zeroes
		for x := lrw.lo; x < blockSize; x++ {
			lrw.buf[x] = 0
		}

		lrw.writeBlock()

		if lrw.err != nil {
			log.WithFields(log.Fields{"error": lrw.err.Error()}).Error("storage::logrecord: next; error in writing the block.")
			return nil, lrw.err
		}
	}

	lrw.lastRecordOffset = lrw.baseOffset + lrw.blockNumber*blockSize + int64(lrw.lo)
	lrw.first = true
	lrw.pending = true
	return singleLogRecordWriter{lrw, lrw.seq}, nil
}

func (lrw *logRecordWriter) close() error {
	return nil
}

type singleLogRecordWriter struct {
	w   *logRecordWriter
	seq int
}

func (slrw singleLogRecordWriter) Write(p []byte) (int, error) {
	panic("Not implemented")
}
