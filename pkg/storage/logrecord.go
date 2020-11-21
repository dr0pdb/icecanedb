package storage

import "io"

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

	// err is any error encountered during any log record writer operation.
	err error
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

func (lrw *logRecordWriter) next() (io.Writer, error) {
	return lrw.w, nil
}

func (lrw *logRecordWriter) close() error {
	return nil
}
