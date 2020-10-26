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
