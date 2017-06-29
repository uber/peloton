package reservation

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/util"
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
