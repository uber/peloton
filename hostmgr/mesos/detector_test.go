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

package mesos

import (
	"fmt"
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/stretchr/testify/suite"
)

type detectorTestSuite struct {
	suite.Suite

	detector zkDetector
}

func (suite *detectorTestSuite) SetupTest() {
	suite.detector = zkDetector{}
}

func (suite *detectorTestSuite) TestDetector() {
	suite.Equal("", suite.detector.HostPort())
	ip := "1.2.3.4"
	var port int32 = 1234
	address := mesos.Address{
		Ip:   &ip,
		Port: &port,
	}
	masterInfo := mesos.MasterInfo{
		Address: &address,
	}
	suite.detector.OnMasterChanged(&masterInfo)
	suite.Equal(fmt.Sprintf("%s:%d", ip, port), suite.detector.HostPort())
}

func TestDetectorTestSuite(t *testing.T) {
	suite.Run(t, new(detectorTestSuite))
}
