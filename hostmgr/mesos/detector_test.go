package mesos

import (
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

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
