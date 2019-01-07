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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/yarpcerrors"
)

func TestConvertToYARPCErrorForYARPCError(t *testing.T) {
	err := ConvertToYARPCError(yarpcerrors.AlreadyExistsErrorf("test error"))
	assert.True(t, yarpcerrors.IsAlreadyExists(err))
}

func TestConvertToYARPCErrorForErrorWithYARPCErrorCause(t *testing.T) {
	err := ConvertToYARPCError(
		errors.Wrap(yarpcerrors.AlreadyExistsErrorf("test error"), "test message"))
	assert.True(t, yarpcerrors.IsAlreadyExists(err))
}

func TestConvertToYARPCErrorForNonYARPCError(t *testing.T) {
	err := ConvertToYARPCError(errors.New("test error"))
	assert.True(t, yarpcerrors.IsInternal(err))
}
