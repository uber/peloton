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
