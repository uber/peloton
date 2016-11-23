package mpb

import "go.uber.org/yarpc/transport"

// Encoding is the name of this encoding.
const (
	Encoding transport.Encoding = "json"

	// ContentTypeApplicationJson header
	ContentTypeJson = "json"

	// ContentTypeApplicationProtobuf header
	ContentTypeProtobuf = "x-protobuf"
)
