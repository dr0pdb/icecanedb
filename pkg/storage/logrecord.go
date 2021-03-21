package storage

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/dr0pdb/icecanedb/internal/common"
	log "github.com/sirupsen/logrus"
)

// The log record format details can be found at the below link.
// https://github.com/google/leveldb/blob/master/doc/log_format.md
//
const (
	blockSize  = 32 * 1024
	headerSize = 7
)

const (
	fullChunkType = iota
	firstChunkType
	middleChunkType
	lastChunkType
)

type flusher interface {
	Flush() error
}

type logRecordReader struct {
	// r is the reader that logRecordReader reads from
	r io.Reader

	// seq is the sequence number of the current record
	seq int

	// buf[lo:hi] is the part of buf that is going to be read next.
	lo, hi int

	// sz is the number of valid bytes. Mostly it'll be blockSize but not for all
	sz int

	// err is any error encountered during any log record reader operation.
	err error

	// buf is the buffer which contains the buffered chunks.
	buf [blockSize]byte

	// nextCalled indicates if next has been called at least once or not. Required in nextChunk to return EOF errors.
	nextCalled bool

	// isLast indicates if the current chunk is the last chunk of the record.
	isLast bool
}

// newLogRecordReader creates a new log record reader.
func newLogRecordReader(r io.Reader) *logRecordReader {
	return &logRecordReader{
		r: r,
	}
}

// nextChunk sets the payload in buf[lo:hi].
// In case it is the last chunk of the current block, it loads the next block in the buffer.
func (lrr *logRecordReader) nextChunk(first bool) error {
	log.WithFields(log.Fields{"seq": lrr.seq, "lo": lrr.lo, "hi": lrr.hi}).Info("storage::logrecord: nextChunk; started.")
	for {
		if lrr.hi+headerSize <= lrr.sz {
			log.WithFields(log.Fields{"seq": lrr.seq, "lo": lrr.lo, "hi": lrr.hi}).Info("storage::logrecord: nextChunk; reading chunk from buffer")
			checksum := binary.LittleEndian.Uint32(lrr.buf[lrr.hi : lrr.hi+4]) // need to change this after checksum implementation.
			length := binary.LittleEndian.Uint16(lrr.buf[lrr.hi+4 : lrr.hi+6])
			chunkType := lrr.buf[lrr.hi+6]

			if checksum == 0 && length == 0 && chunkType == 0 {
				if first {
					continue
				}
				return errors.New("buffer zeroed out..")
			}

			lrr.lo = lrr.hi + headerSize
			lrr.hi = lrr.hi + headerSize + int(length)
			if lrr.hi > lrr.sz {
				return errors.New("hi greater than sz. invalid position of hi.")
			}

			if first {
				if chunkType != fullChunkType && chunkType != firstChunkType {
					// we wanted first this chunk is not the first. so keep looping
					log.Info("storage::logrecord: nextChunk; expected first chunk of a record but this is not. skipping..")
					continue
				}
			}

			lrr.isLast = chunkType == fullChunkType || chunkType == lastChunkType
			return nil
		}

		if lrr.sz < blockSize && lrr.nextCalled {
			if lrr.hi != lrr.sz {
				log.Info("storage::logrecord: nextChunk; expecting the contents to be till hi but the content is only till sz")
				return io.ErrUnexpectedEOF
			}
			return io.EOF
		}

		log.Info("storage::logrecord: nextChunk; we have reached the end of the current block, so read the next block to buf and reset lo, hi and sz.")
		sz, err := io.ReadFull(lrr.r, lrr.buf[:])
		if err != nil && err != io.ErrUnexpectedEOF {
			log.Error("storage::logrecord: nextChunk; Unexpected error while reading from log file")
			return err
		}
		lrr.lo, lrr.hi, lrr.sz = 0, 0, sz
	}
}

func (lrr *logRecordReader) next() (io.Reader, error) {
	log.WithFields(log.Fields{"seq": lrr.seq, "lo": lrr.lo, "hi": lrr.hi}).Info("storage::logrecord: next; started.")
	lrr.seq++
	if lrr.err != nil {
		return nil, lrr.err
	}

	lrr.lo = lrr.hi
	lrr.nextCalled = true
	lrr.err = lrr.nextChunk(true)
	if lrr.err != nil {
		return nil, lrr.err
	}
	log.WithFields(log.Fields{"seq": lrr.seq, "lo": lrr.lo, "hi": lrr.hi}).Info("storage::logrecord: next; done.")
	return singleLogRecordReader{lrr, lrr.seq}, nil
}

type singleLogRecordReader struct {
	r   *logRecordReader
	seq int
}

func (slrr singleLogRecordReader) Read(p []byte) (int, error) {
	log.WithFields(log.Fields{"seq": slrr.seq}).Info("storage::logrecord: Read; started.")
	r := slrr.r

	if r.seq != slrr.seq {
		log.WithFields(log.Fields{"seq": slrr.seq, "rseq": r.seq}).Error("storage::logrecord: Read; stale log record reader")
		return 0, common.NewStaleLogRecordWriterError("Stale Log Record reader state")
	}

	if r.err != nil {
		log.Error("storage::logrecord: Read; existing error. returning it.")
		return 0, r.err
	}

	// skip empty chunks
	for r.lo == r.hi {
		log.Info("storage::logrecord: Read; skipping empty chunk.")
		if r.isLast {
			return 0, io.EOF
		}

		if r.err = r.nextChunk(false); r.err != nil {
			return 0, r.err
		}
	}
	log.WithFields(log.Fields{"r.lo": r.lo, "r.hi": r.hi}).Info("storage::logrecord: Read; copy from r.buf to p")
	n := copy(p, r.buf[r.lo:r.hi])
	r.lo = n
	return n, nil
}

type logRecordWriter struct {
	// w is the writer that logRecordWriter writes to
	w io.Writer

	// seq is the sequence number of the current record.
	seq int

	// f is the writer as the flusher.
	f flusher

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

// fillHeaders fill the header entry in the buffer for the current chunk.
func (lrw *logRecordWriter) fillHeaders(lastChunk bool) {
	log.WithFields(log.Fields{"lastChunk": lastChunk, "seq": lrw.seq, "lo": lrw.lo, "hi": lrw.hi}).Info("storage::logrecord: fillHeaders; fillHeaders called.")

	if lrw.err != nil {
		log.WithFields(log.Fields{"error": lrw.err.Error()}).Error("storage::logrecord: fillHeaders; existing background error found in the log record writer.")
		return
	}

	if lrw.lo+headerSize > lrw.hi || lrw.hi > blockSize {
		log.WithFields(log.Fields{"lo": lrw.lo, "hi": lrw.hi}).Error("storage::logrecord: fillHeaders; Inconsistent state found.")
		panic("storage::logrecord::logrecordwriter; inconsistent state found")
	}

	if lastChunk {
		if lrw.first {
			lrw.buf[lrw.lo+6] = fullChunkType
		} else {
			lrw.buf[lrw.lo+6] = lastChunkType
		}
	} else {
		if lrw.first {
			lrw.buf[lrw.lo+6] = firstChunkType
		} else {
			lrw.buf[lrw.lo+6] = middleChunkType
		}
	}

	binary.LittleEndian.PutUint32(lrw.buf[lrw.lo:lrw.lo+4], 0)                                  // checksum 0 for now.
	binary.LittleEndian.PutUint16(lrw.buf[lrw.lo+4:lrw.lo+6], uint16(lrw.hi-lrw.lo-headerSize)) // length of payload

	log.Info("storage::logrecord: fillHeaders; fillHeaders done.")
}

func (lrw *logRecordWriter) writePending() {
	log.WithFields(log.Fields{"seq": lrw.seq, "lo": lrw.lo, "hi": lrw.hi}).Info("storage::logrecord: writePending; writePending called.")

	if lrw.err != nil {
		log.Info("storage::logrecord: writePending; found existing error.")
		return
	}
	if lrw.pending {
		lrw.fillHeaders(true)
		lrw.pending = false
	}

	_, lrw.err = lrw.w.Write(lrw.buf[lrw.sofar:lrw.hi])
	lrw.sofar = lrw.hi

	log.Info("storage::logrecord: writePending; done.")
}

func (lrw *logRecordWriter) writeBlock() {
	log.WithFields(log.Fields{"seq": lrw.seq, "lo": lrw.lo, "hi": lrw.hi}).Info("storage::logrecord: writeBlock; start.")

	_, lrw.err = lrw.w.Write(lrw.buf[lrw.sofar:])
	lrw.lo = 0
	lrw.hi = headerSize
	lrw.sofar = 0
	lrw.blockNumber++
	log.Info("storage::logrecord: writeBlock; done.")
}

func (lrw *logRecordWriter) flush() error {
	log.WithFields(log.Fields{"seq": lrw.seq, "lo": lrw.lo, "hi": lrw.hi}).Info("storage::logrecord: flush; start.")
	lrw.seq++
	lrw.writePending()

	if lrw.err != nil {
		log.Info("storage::logrecord: flush; error in writing pending. returning without flushing")
		return lrw.err
	}

	if lrw.f != nil {
		log.Info("storage::logrecord: flush; flushing.")
		lrw.err = lrw.f.Flush()
	}

	log.Info("storage::logrecord: flush; done.")
	return lrw.err
}

// newLogRecordWriter creates a new log record writer.
// IMP: It seeks to the end of the writer for appending new entries.
func newLogRecordWriter(w io.Writer) *logRecordWriter {
	var offset int64
	if s, ok := w.(io.Seeker); ok {
		var err error
		if offset, err = s.Seek(0, io.SeekEnd); err != nil {
			offset = 0
		}
	}

	f, _ := w.(flusher)

	return &logRecordWriter{
		w:                w,
		baseOffset:       offset,
		lastRecordOffset: -1,
		f:                f,
	}
}

// next returns a io.Writer for the next record.
func (lrw *logRecordWriter) next() (io.Writer, error) {
	lrw.seq++
	if lrw.err != nil {
		log.WithFields(log.Fields{"error": lrw.err.Error()}).Error("storage::logrecord: next; existing background error found in the log record writer.")
		return nil, lrw.err
	}

	log.WithFields(log.Fields{"seq": lrw.seq, "lo": lrw.lo, "hi": lrw.hi}).Info("storage::logrecord: next; next called on the log record writer.")

	if lrw.pending {
		log.Info("storage::logrecord: next; found pending chunk. It's corresponding writer will be invalidated.")
		lrw.fillHeaders(true)
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
	log.WithFields(log.Fields{"seq": lrw.seq, "lo": lrw.lo, "hi": lrw.hi}).Info("storage::logrecord: close; start.")

	lrw.seq++
	lrw.writePending()
	if lrw.err != nil {
		log.Info("storage::logrecord: close; error in writing pending chunk.")
		return lrw.err
	}
	lrw.err = common.NewStaleLogRecordWriterError("close writer")

	log.Info("storage::logrecord: close; done.")
	return nil
}

type singleLogRecordWriter struct {
	w   *logRecordWriter
	seq int
}

// Write writes a slice of byte to the writer by splitting it into blocks of blocksize.
func (slrw singleLogRecordWriter) Write(p []byte) (int, error) {
	log.WithFields(log.Fields{"p": string(p)}).Info("storage::logrecord: Write; start.")
	w := slrw.w

	if w.seq != slrw.seq {
		return 0, common.NewStaleLogRecordWriterError("Stale Log Record Writer state")
	}

	if w.err != nil {
		return 0, w.err
	}

	tot := len(p)
	for len(p) > 0 {
		// write if full
		if w.hi == blockSize {
			w.fillHeaders(false)
			w.writeBlock()

			if w.err != nil {
				return 0, w.err
			}

			w.first = false
		}

		n := copy(w.buf[w.hi:], p)
		w.hi += n
		p = p[n:]
	}

	log.Info("storage::logrecord: Write; done.")
	return tot, nil
}
