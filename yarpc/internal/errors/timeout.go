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

import (
	"fmt"
	"time"
)

// TimeoutError indicates that an error occurred due to a context deadline over
// the course of a request over any transport.
type TimeoutError interface {
	error

	timeoutError()
}

// handlerTimeoutError represents a timeout on the handler side.
type handlerTimeoutError struct {
	Caller    string
	Service   string
	Procedure string
	Duration  time.Duration
}

var _ TimeoutError = handlerTimeoutError{}
var _ HandlerError = handlerTimeoutError{}

// HandlerTimeoutError constructs an instance of a TimeoutError representing
// a timeout that occurred during the handler execution, with the caller,
// service, procedure and duration waited.
func HandlerTimeoutError(Caller string, Service string, Procedure string, Duration time.Duration) error {
	return handlerTimeoutError{
		Caller:    Caller,
		Service:   Service,
		Procedure: Procedure,
		Duration:  Duration,
	}
}

func (handlerTimeoutError) timeoutError() {}
func (handlerTimeoutError) handlerError() {}

func (e handlerTimeoutError) Error() string {
	return fmt.Sprintf(`Timeout: call to procedure %q of service %q from caller %q timed out after %v`,
		e.Procedure, e.Service, e.Caller, e.Duration)
}

// RemoteTimeoutError represents a TimeoutError from a remote handler.
type RemoteTimeoutError string

var _ TimeoutError = RemoteTimeoutError("")

func (RemoteTimeoutError) timeoutError() {}

func (e RemoteTimeoutError) Error() string {
	return string(e)
}

// clientTimeoutError represents a timeout on the client side.
type clientTimeoutError struct {
	Service   string
	Procedure string
	Duration  time.Duration
}

var _ TimeoutError = clientTimeoutError{}

// ClientTimeoutError constructs an instance of a TimeoutError representing
// a timeout that occurred while the client was waiting during a request to a
// remote handler. It includes the service, procedure and duration waited.
func ClientTimeoutError(Service string, Procedure string, Duration time.Duration) error {
	return clientTimeoutError{
		Service:   Service,
		Procedure: Procedure,
		Duration:  Duration,
	}
}

func (clientTimeoutError) timeoutError() {}
func (clientTimeoutError) clientError()  {}

func (e clientTimeoutError) Error() string {
	return fmt.Sprintf(`client timeout for procedure %q of service %q after %v`,
		e.Procedure, e.Service, e.Duration)
}
