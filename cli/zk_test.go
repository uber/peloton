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

package cli

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

const (
	testClusterName        = "pit1-preprod01"
	testInvalidClusterName = "invalidClusterName"
	testZKURL              = "zookeeper-mesos-preprod01.pit-irn-1.uberatc.net:2181"
)

// positive TC : valid file name and valid cluster name
func TestGetValidZkURLValidClusterName(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testClusterName)
	assert.Nil(t, err)
	expected := testZKURL
	assert.Equal(t, expected, string(zkURL))
}

// negative TC: cluster is not in the config file
func TestInvalidClusterName(t *testing.T) {
	zkURL, err := GetZkInfoFromClusterName(testInvalidClusterName)
	assert.NotNil(t, err)
	assert.EqualValues(t, zkURL, "", "response should be empty string")
	expectedErr := "cannot find the corresponding zk url for " + testInvalidClusterName
	assert.EqualError(t, err, expectedErr)
}

// negative TC: could not find the config file
func TestNoConfigFile(t *testing.T) {
	filePath := "."
	fileName := "test"
	zkURL, err := readConfigFile(filePath, fileName)
	assert.NotNil(t, err)
	assert.EqualValues(t, zkURL, "", "response should be empty string")
	expectedErr := "unable to open file " + fileName
	assert.EqualError(t, err, expectedErr)
}
