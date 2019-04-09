package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/yarpcerrors"
)

const _testFailureMsg = "error is part of API, changes in error would be a breaking change"

func TestErrorCode(t *testing.T) {
	assert.True(t, yarpcerrors.IsAborted(UnexpectedVersionError), _testFailureMsg)
	assert.True(t, yarpcerrors.IsAborted(InvalidEntityVersionError), _testFailureMsg)
}

func TestErrorMessage(t *testing.T) {
	assert.Equal(t,
		yarpcerrors.FromError(UnexpectedVersionError).Message(),
		"operation aborted due to unexpected version",
		_testFailureMsg,
	)

	assert.Equal(t,
		yarpcerrors.FromError(InvalidEntityVersionError).Message(),
		"unexpected entity version",
		_testFailureMsg,
	)
}
