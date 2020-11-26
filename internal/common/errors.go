package common

import (
	"fmt"
)

// NotFoundError is returned when the required value is not found.
type NotFoundError struct {
	Message string
}

func (nf NotFoundError) Error() string {
	return fmt.Sprintf("%s", nf.Message)
}

// NewNotFoundError creates a new instance of NotFoundError with the given message.
func NewNotFoundError(message string) NotFoundError {
	return NotFoundError{
		Message: message,
	}
}

// UnknownError is returned when an unknown error happens.
type UnknownError struct {
	Message string
}

func (nf UnknownError) Error() string {
	return fmt.Sprintf("%s", nf.Message)
}

// NewUnknownError creates a new instance of UnknownError with the given message.
func NewUnknownError(message string) UnknownError {
	return UnknownError{
		Message: message,
	}
}

// StaleLogRecordWriterError is returned when the log record writer is in stale state.
type StaleLogRecordWriterError struct {
	Message string
}

func (slrw StaleLogRecordWriterError) Error() string {
	return fmt.Sprintf("%s", slrw.Message)
}

// NewStaleLogRecordWriterError creates a new instance of StaleLogRecordWriterError with the given message.
func NewStaleLogRecordWriterError(message string) StaleLogRecordWriterError {
	return StaleLogRecordWriterError{
		Message: message,
	}
}
