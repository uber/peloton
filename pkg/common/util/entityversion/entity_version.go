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

package entityversion

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"

	"go.uber.org/yarpc/yarpcerrors"
)

// GetJobEntityVersion builds the job entity version from its components
func GetJobEntityVersion(
	configVersion uint64,
	desiredStateVersion uint64,
	workflowVersion uint64,
) *v1alphapeloton.EntityVersion {
	return &v1alphapeloton.EntityVersion{
		Value: fmt.Sprintf("%d-%d-%d", configVersion, desiredStateVersion, workflowVersion),
	}
}

// ParseJobEntityVersion parses job entity version into components
func ParseJobEntityVersion(
	entityVersion *v1alphapeloton.EntityVersion,
) (configVersion uint64, desiredStateVersion uint64, workflowVersion uint64, err error) {
	results := strings.Split(entityVersion.GetValue(), "-")
	if len(results) != 3 {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format")
		return
	}

	configVersion, err = strconv.ParseUint(results[0], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}

	desiredStateVersion, err = strconv.ParseUint(results[1], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}

	workflowVersion, err = strconv.ParseUint(results[2], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}
	return
}

// GetPodEntityVersion builds the pod entity version from its components
func GetPodEntityVersion(configVersion uint64) *v1alphapeloton.EntityVersion {
	b := bytes.Buffer{}
	b.WriteString(strconv.FormatUint(configVersion, 10))
	b.WriteString("-0-0")
	return &v1alphapeloton.EntityVersion{
		Value: b.String(),
	}
}

// GetConfigVersion parses pod/job entity version to
// get the job config version
func GetConfigVersion(
	entityVersion *v1alphapeloton.EntityVersion,
) (configVersion uint64, err error) {
	results := strings.Split(entityVersion.GetValue(), "-")
	if len(results) != 3 {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format")
		return
	}

	configVersion, err = strconv.ParseUint(results[0], 10, 64)
	if err != nil {
		err = yarpcerrors.InvalidArgumentErrorf("entityVersion has wrong format: %s", err.Error())
	}

	return
}
