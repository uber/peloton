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

package reservation

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/common/util"
)

const (
	_testJob      = "testjob"
	_testInstance = 0
	_testHostname = "hostname-0"
)

type LabelTestSuite struct {
	suite.Suite
}

func (suite *LabelTestSuite) SetupTest() {
}

func (suite *LabelTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestOperationTestSuite(t *testing.T) {
	suite.Run(t, new(LabelTestSuite))
}

func (suite *LabelTestSuite) TestCreateParseReservationLabel() {
	reservationLabels := CreateReservationLabels(_testJob, _testInstance, _testHostname)
	jobID, instanceID, err := ParseReservationLabels(reservationLabels)
	suite.NoError(err)
	suite.Equal(jobID, _testJob)
	suite.Equal(instanceID, uint32(_testInstance))
}

func (suite *LabelTestSuite) TestParseReservationLabelWithInstanceIDMissingError() {
	reservationLabels := &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_jobKey,
				Value: util.PtrPrintf(_testJob),
			},
		},
	}
	_, _, err := ParseReservationLabels(reservationLabels)
	suite.Error(err)
}
