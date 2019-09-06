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

package thermos

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"

	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/pkg/errors"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/ptr"
)

// MetadataByKey sorts a list of Aurora Metadata by key
type MetadataByKey []*api.Metadata

func (m MetadataByKey) Len() int {
	return len(m)
}

func (m MetadataByKey) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m MetadataByKey) Less(i, j int) bool {
	return strings.Compare(m[i].GetKey(), m[j].GetKey()) < 0
}

// ResourceByType sorts a list of Aurora Resource by resource type, and port
// name is used when sorting mulitple NamedPort resource
type ResourceByType []*api.Resource

func (r ResourceByType) Len() int {
	return len(r)
}

func (r ResourceByType) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r ResourceByType) Less(i, j int) bool {
	indexI := resourceIndex(*r[i]).Index()
	indexJ := resourceIndex(*r[j]).Index()

	if indexI != indexJ {
		return indexI < indexJ
	}

	// only expect same index if it's NamedPort
	if len(r[i].GetNamedPort()) > 0 && len(r[j].GetNamedPort()) > 0 {
		// sort named ports by value
		return strings.Compare(r[i].GetNamedPort(), r[j].GetNamedPort()) < 0
	}

	// error case, but return False here to keep original ordering
	return false
}

type resourceIndex api.Resource

func (r resourceIndex) Index() int {
	if r.NumCpus != nil {
		return 0
	}
	if r.RamMb != nil {
		return 1
	}
	if r.DiskMb != nil {
		return 2
	}
	if r.NamedPort != nil {
		return 3
	}
	if r.NumGpus != nil {
		return 4
	}
	return -1
}

// ConstraintByName sorts a list of Aurora Constraint by name
type ConstraintByName []*api.Constraint

func (c ConstraintByName) Len() int {
	return len(c)
}

func (c ConstraintByName) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ConstraintByName) Less(i, j int) bool {
	return strings.Compare(c[i].GetName(), c[j].GetName()) < 0
}

// MesosFetcherURIByValue sorts a list of Aurora MesosFetcherURI by value
type MesosFetcherURIByValue []*api.MesosFetcherURI

func (m MesosFetcherURIByValue) Len() int {
	return len(m)
}

func (m MesosFetcherURIByValue) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m MesosFetcherURIByValue) Less(i, j int) bool {
	return strings.Compare(m[i].GetValue(), m[j].GetValue()) < 0
}

func EncodeTaskConfig(t *api.TaskConfig) ([]byte, error) {
	// Sort golang list types in order to make results consistent
	sort.Stable(MetadataByKey(t.Metadata))
	sort.Stable(ResourceByType(t.Resources))
	sort.Stable(ConstraintByName(t.Constraints))
	sort.Stable(MesosFetcherURIByValue(t.MesosFetcherUris))

	if len(t.GetExecutorConfig().GetData()) != 0 {
		var dat map[string]interface{}

		err := json.Unmarshal([]byte(t.GetExecutorConfig().GetData()), &dat)
		if err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal executor data json")
		}

		data, err := json.MarshalIndent(dat, "", "")
		if err != nil {
			return nil, errors.Wrap(err, "failed to re-marshal executor data to json")
		}

		t.ExecutorConfig.Data = ptr.String(string(data))
	}

	w, err := t.ToWire()
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert task config to wire value")
	}

	var b bytes.Buffer
	err = protocol.Binary.Encode(w, &b)
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize task config to binary")
	}

	return b.Bytes(), nil
}
