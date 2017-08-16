package goalstate

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

var (
	_testJobID = uuid.NewUUID().String()
)

type TrackerTestSuite struct {
	suite.Suite

	mockCtrl *gomock.Controller
	tracker  *tracker
}

func (suite *TrackerTestSuite) SetupTest() {
	suite.tracker = newTracker()
}

func TestTrackerTestSuite(t *testing.T) {
	suite.Run(t, new(TrackerTestSuite))
}

func (suite *TrackerTestSuite) TestTrackerJobs() {
	jobID := &peloton.JobID{
		Value: _testJobID,
	}

	t := suite.tracker.addOrGetJob(jobID, nil)
	suite.NotNil(t)

	suite.Equal(t, suite.tracker.addOrGetJob(jobID, nil))
	suite.Equal(t, suite.tracker.getJob(jobID))
}
