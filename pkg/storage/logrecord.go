package storage

type logRecordWriter struct{}

// newLogRecordWriter creates a new log record writer.
func newLogRecordWriter() *logRecordWriter {
	return &logRecordWriter{}
}
