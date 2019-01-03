package label

import (
	"encoding/json"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
)

const _auroraMetadataKey = "aurora_metadata"

// AuroraMetadata is a label for the original Aurora task metadata which was
// mapped into a job.
type AuroraMetadata struct {
	data string
}

// NewAuroraMetadata creates a new AuroraMetadata label.
func NewAuroraMetadata(md []*api.Metadata) (*AuroraMetadata, error) {
	b, err := json.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %s", err)
	}
	return &AuroraMetadata{string(b)}, nil
}

// Key returns the label key.
func (m *AuroraMetadata) Key() string {
	return _auroraMetadataKey
}

// Value returns the label value.
func (m *AuroraMetadata) Value() string {
	return m.data
}
