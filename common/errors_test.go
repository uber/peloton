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
