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

// UnexpectedError is a server failure due to unhandled errors. This can be
// caused if the remote server panics while processing the request or fails to
// handle any other errors.
type UnexpectedError interface {
	error

	unexpectedError()
}

type handlerUnexpectedError struct {
	Reason error
}

var _ UnexpectedError = handlerUnexpectedError{}
var _ HandlerError = handlerUnexpectedError{}

// HandlerUnexpectedError wraps the given error into an UnexpectedError.
//
// It represens a local failure while processing a request.
func HandlerUnexpectedError(err error) HandlerError {
	return handlerUnexpectedError{Reason: err}
}

func (handlerUnexpectedError) handlerError()    {}
func (handlerUnexpectedError) unexpectedError() {}

func (e handlerUnexpectedError) Error() string {
	return "UnexpectedError: " + e.Reason.Error()
}

type remoteUnexpectedError string

var _ UnexpectedError = remoteUnexpectedError("")

// RemoteUnexpectedError builds a new UnexpectedError with the given message.
//
// It represents an UnexpectedError from a remote service.
func RemoteUnexpectedError(message string) UnexpectedError {
	return remoteUnexpectedError(message)
}

func (remoteUnexpectedError) unexpectedError() {}

func (e remoteUnexpectedError) Error() string {
	return string(e)
}
