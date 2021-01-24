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

// AbortedTransactionError is returned when an operation is called on an aborted txn.
type AbortedTransactionError struct {
	Message string
}

func (ate AbortedTransactionError) Error() string {
	return fmt.Sprintf("%s", ate.Message)
}

// NewAbortedTransactionError creates a new instance of AbortedTransactionError with the given message.
func NewAbortedTransactionError(message string) AbortedTransactionError {
	return AbortedTransactionError{
		Message: message,
	}
}

// CommittedTransactionError is returned when an operation is called on an already committed txn.
type CommittedTransactionError struct {
	Message string
}

func (ate CommittedTransactionError) Error() string {
	return fmt.Sprintf("%s", ate.Message)
}

// NewCommittedTransactionError creates a new instance of CommittedTransactionError with the given message.
func NewCommittedTransactionError(message string) CommittedTransactionError {
	return CommittedTransactionError{
		Message: message,
	}
}

// TransactionCommitError is returned when a commit operation fails on a txn.
type TransactionCommitError struct {
	Message string
}

func (ate TransactionCommitError) Error() string {
	return fmt.Sprintf("%s", ate.Message)
}

// NewTransactionCommitError creates a new instance of TransactionCommitError with the given message.
func NewTransactionCommitError(message string) TransactionCommitError {
	return TransactionCommitError{
		Message: message,
	}
}
