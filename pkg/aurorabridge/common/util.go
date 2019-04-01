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

package common

import (
	"fmt"

	"github.com/uber/peloton/pkg/common/util"

	"go.uber.org/thriftrw/ptr"
)

// ConvertTimestampToUnixMS converts Peloton event timestamp string
// (in seconds) to unix time in milliseconds used by Bridge.
func ConvertTimestampToUnixMS(timestamp string) (*int64, error) {
	u, err := util.ConvertTimestampToUnixSeconds(timestamp)
	if err != nil {
		return nil, fmt.Errorf("unable to parse timestamp %s", err)
	}
	return ptr.Int64(u * 1000), nil
}
