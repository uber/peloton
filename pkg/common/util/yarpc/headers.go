package yarpc

import (
	"context"

	"go.uber.org/yarpc"
)

// GetHeaders returns all the yarpc headers in the context
func GetHeaders(ctx context.Context) map[string]string {
	result := make(map[string]string)
	call := yarpc.CallFromContext(ctx)
	for _, header := range call.HeaderNames() {
		result[header] = call.Header(header)
	}
	return result
}
