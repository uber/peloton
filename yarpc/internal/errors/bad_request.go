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

// BadRequestError is a failure to process a request because the request was
// invalid.
type BadRequestError interface {
	error

	badRequestError()
}

type handlerBadRequestError struct {
	Reason error
}

var _ BadRequestError = handlerBadRequestError{}
var _ HandlerError = handlerBadRequestError{}

// HandlerBadRequestError wraps the given error into a BadRequestError.
//
// It represents a local failure while processing an invalid request.
func HandlerBadRequestError(err error) HandlerError {
	return handlerBadRequestError{Reason: err}
}

func (handlerBadRequestError) handlerError()    {}
func (handlerBadRequestError) badRequestError() {}

func (e handlerBadRequestError) Error() string {
	return "BadRequest: " + e.Reason.Error()
}

type remoteBadRequestError string

var _ BadRequestError = remoteBadRequestError("")

// RemoteBadRequestError builds a new BadRequestError with the given message.
//
// It represents a BadRequest failure from a remote service.
func RemoteBadRequestError(message string) BadRequestError {
	return remoteBadRequestError(message)
}

func (remoteBadRequestError) badRequestError() {}

func (e remoteBadRequestError) Error() string {
	return string(e)
}
