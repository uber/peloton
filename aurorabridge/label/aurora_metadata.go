// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package label

import (
	"encoding/json"
	"fmt"

	"github.com/uber/peloton/.gen/thrift/aurora/api"
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
