package mpb

import "go.uber.org/yarpc/api/transport"

// Encoding is the name of this encoding.
const (
	Encoding transport.Encoding = "json"

	// ContentTypeJSON header
	ContentTypeJSON = "json"

	// ContentTypeProtobuf header
	ContentTypeProtobuf = "x-protobuf"
)
