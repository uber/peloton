package handler

import (
	"github.com/pkg/errors"
	"go.uber.org/yarpc/yarpcerrors"
)

// ConvertToYARPCError converts an error to
// yarpc error with correct status code
func ConvertToYARPCError(err error) error {
	// err is yarpc error, directly return the err
	if yarpcerrors.IsStatus(err) {
		return err
	}

	// if the cause of the error is yarpc error, retain the
	// error code. Otherwise, use internal error code.
	statusCode := yarpcerrors.CodeInternal
	if yarpcerrors.IsStatus(errors.Cause(err)) {
		statusCode = errors.Cause(err).(*yarpcerrors.Status).Code()
	}
	return yarpcerrors.Newf(statusCode, err.Error())
}
