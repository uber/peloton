package common

import (
	"go.uber.org/yarpc/yarpcerrors"
)

// IsTransientError returns true if the error was transient and the overall
// operation should be retried.
func IsTransientError(err error) bool {
	if yarpcerrors.IsAlreadyExists(err) {
		return true
	}
	if yarpcerrors.IsAborted(err) {
		return true
	}

	if yarpcerrors.IsUnavailable(err) {
		return true
	}

	if yarpcerrors.IsDeadlineExceeded(err) {
		return true
	}

	return false
}
