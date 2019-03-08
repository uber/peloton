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
