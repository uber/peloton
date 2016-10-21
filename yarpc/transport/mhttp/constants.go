package mhttp

const (
	// ApplicationHeaderPrefix is the prefix added to application headers over
	// the wire.
	ApplicationHeaderPrefix = "Rpc-Header-"

	// BaggageHeaderPrefix is the prefix added to context headers over the wire.
	BaggageHeaderPrefix = "Context-"

	// TODO(abg): Allow customizing header prefixes

	// CallerHeader is the HTTP header used to indiate the service doing the calling
	CallerHeader = "Rpc-Caller"

	// EncodingHeader is the HTTP header used to specify the name of the
	// encoding.
	EncodingHeader = "Rpc-Encoding"

	// TTLMSHeader is the HTTP header used to indicate the ttl in ms
	TTLMSHeader = BaggageHeaderPrefix + "TTL-MS"

	// ProcedureHeader is the HTTP header used to indicate the procedure
	ProcedureHeader = "Rpc-Procedure"

	// ServiceHeader is the HTTP header used to indicate the service
	ServiceHeader = "Rpc-Service"
)
