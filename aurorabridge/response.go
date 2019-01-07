// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aurorabridge

import (
	"fmt"

	"github.com/uber/peloton/.gen/thrift/aurora/api"
	"go.uber.org/thriftrw/ptr"
)

// auroraError is a utility for building Aurora errors.
type auroraError struct {
	responseCode api.ResponseCode
	msg          string
}

func auroraErrorf(format string, args ...interface{}) *auroraError {
	return &auroraError{
		responseCode: api.ResponseCodeError,
		msg:          fmt.Sprintf(format, args...),
	}
}

func (e *auroraError) code(c api.ResponseCode) *auroraError {
	e.responseCode = c
	return e
}

// newResponse is a convenience wrapper for converting a result and error into
// a Response. r is ignored on non-nil errs, but extraDetails are always added
// regardless of err.
func newResponse(r *api.Result, err *auroraError, extraDetails ...string) *api.Response {
	if err != nil {
		return &api.Response{
			ResponseCode: err.responseCode.Ptr(),
			Details:      newResponseDetails(append(extraDetails, err.msg)...),
		}
	}
	return &api.Response{
		ResponseCode: api.ResponseCodeOk.Ptr(),
		Result:       r,
		Details:      newResponseDetails(extraDetails...),
	}
}

func newResponseDetails(messages ...string) []*api.ResponseDetail {
	var ds []*api.ResponseDetail
	for _, m := range messages {
		ds = append(ds, &api.ResponseDetail{Message: ptr.String(m)})
	}
	return ds
}
