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

package common

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/yarpcerrors"
)

func TestIsTransientError(t *testing.T) {
	assert.True(t, IsTransientError(yarpcerrors.AlreadyExistsErrorf("")))
	assert.True(t, IsTransientError(yarpcerrors.DeadlineExceededErrorf("")))
	// An explicit context exceeded error should not be retried, as it's not a
	// transient error.
	assert.False(t, IsTransientError(context.DeadlineExceeded))
	assert.False(t, IsTransientError(context.Canceled))
	assert.False(t, IsTransientError(nil))
	assert.True(t, IsTransientError(yarpcerrors.AbortedErrorf("aborted")))
	assert.True(t, IsTransientError(yarpcerrors.UnavailableErrorf("Unavailable")))
}
