package yarpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/yarpc/api/encoding"
	"go.uber.org/yarpc/api/transport"
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
