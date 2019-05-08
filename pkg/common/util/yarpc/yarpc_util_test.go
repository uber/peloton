package yarpc

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/api/encoding"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

func TestGetHeaders(t *testing.T) {
	headersMap := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}

	ctx, inboundCall := encoding.NewInboundCall(context.Background())
	err := inboundCall.ReadFromRequest(
		&transport.Request{
			Headers: transport.HeadersFromMap(headersMap),
		},
	)
	assert.NoError(t, err)
	assert.Equal(t, GetHeaders(ctx), headersMap)
}

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
