// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package errors

import "fmt"

// HandlerError represents handler errors on the handler side.
//
// The general hierarchy we have is:
//
// 	BadRequestError                   HandlerError
// 	 |                                        |
// 	 +--------> handlerBadRequestError <------+
// 	 |                                        |
// 	 +--------> remoteBadRequestError         |
// 	                                          |
// 	UnexpectedError                           |
// 	 |                                        |
// 	 +--------> handlerUnexpectedError <------+
// 	 |                                        |
// 	 +--------> remoteUnexpectedError         |
// 	                                          |
// 	TimeoutError                              |
// 	 |                                        |
// 	 +--------> handlerTimeoutError <---------+
// 	 |
// 	 +--------> remoteTimeoutError
//   |
// 	 +--------> clientTimeoutError
//
// Only the handler versions of the error types are HandlerErrors. If a handler
// returns one of the remote or client version, they will be wrapped in a new
// UnexpectedError.
//
// HandlerError represents error types that can be returned from a handler that
// the Inbound implementation MUST handle. The inbound implementation can then
// returns the remote version of the error to the user.
//
// Currently, this includes BadRequestError, UnexpectedError and TimeoutError.
// Error types which know how to convert themselves into BadRequestError,
// UnexpectedError or TimeoutError may provide a `AsHandlerError()
// HandlerError` method.
type HandlerError interface {
	error

	handlerError()
}

type asHandlerError interface {
	AsHandlerError() HandlerError
}

// AsHandlerError converts an error into a HandlerError, leaving it unchanged
// if it is already one.
func AsHandlerError(service, procedure string, err error) error {
	if err == nil {
		return err
	}

	switch e := err.(type) {
	case HandlerError:
		return e
	case asHandlerError:
		return e.AsHandlerError()
	default:
		return ProcedureFailedError{
			Service:   service,
			Procedure: procedure,
			Reason:    err,
		}.AsHandlerError()
	}
}

// UnrecognizedProcedureError is a failure to process a request because the
// procedure and/or service name was unrecognized.
type UnrecognizedProcedureError struct {
	Service   string
	Procedure string
}

func (e UnrecognizedProcedureError) Error() string {
	return fmt.Sprintf(`unrecognized procedure %q for service %q`, e.Procedure, e.Service)
}

// AsHandlerError for UnrecognizedProcedureError.
func (e UnrecognizedProcedureError) AsHandlerError() HandlerError {
	return HandlerBadRequestError(e)
}

// ProcedureFailedError is a failure to execute a procedure due to an
// unexpected error.
type ProcedureFailedError struct {
	Service   string
	Procedure string
	Reason    error
}

func (e ProcedureFailedError) Error() string {
	return fmt.Sprintf(`error for procedure %q of service %q: %v`,
		e.Procedure, e.Service, e.Reason)
}

// AsHandlerError for ProcedureFailedError.
func (e ProcedureFailedError) AsHandlerError() HandlerError {
	return HandlerUnexpectedError(e)
}
