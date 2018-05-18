package host

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"

	"code.uber.internal/infra/peloton/util"
	mock_mpb "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

var (
	_cpuName  = "cpus"
	_memName  = "mem"
	_diskName = "disk"
	_gpuName  = "gpus"

	_defaultResourceValue = 1
)

type HostMapTestSuite struct {
	suite.Suite

	ctrl           *gomock.Controller
	testScope      tally.TestScope
	operatorClient *mock_mpb.MockMasterOperatorClient
}

func (suite *HostMapTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.operatorClient = mock_mpb.NewMockMasterOperatorClient(suite.ctrl)
}

func makeAgentsResponse(numAgents int) *mesos_master.Response_GetAgents {
	response := &mesos_master.Response_GetAgents{
		Agents: []*mesos_master.Response_GetAgents_Agent{},
	}
	for i := 0; i < numAgents; i++ {
		resVal := float64(_defaultResourceValue)
		tmpID := fmt.Sprintf("id-%d", i)
		resources := []*mesos.Resource{
			util.NewMesosResourceBuilder().
				WithName(_cpuName).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(_memName).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(_diskName).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(_gpuName).
				WithValue(resVal).
				Build(),
		}
		getAgent := &mesos_master.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Id: &mesos.AgentID{
					Value: &tmpID,
				},
				Resources: resources,
			},
		}
		response.Agents = append(response.Agents, getAgent)
	}

	return response
}

func (suite *HostMapTestSuite) TestRefresh() {
	defer suite.ctrl.Finish()

	loader := &Loader{
		OperatorClient: suite.operatorClient,
		Scope:          suite.testScope,
	}
	numAgents := 2
	response := makeAgentsResponse(numAgents)
	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(response, nil),
	)

	loader.Load(nil)

	m := GetAgentMap()
	suite.Len(m.RegisteredAgents, numAgents)

	id1 := "id-1"
	id2 := "id-2"
	a1 := GetAgentInfo(&mesos.AgentID{Value: &id1})
	suite.NotEmpty(a1.Resources)
	a2 := GetAgentInfo(&mesos.AgentID{Value: &id2})
	suite.Nil(a2)

	gauges := suite.testScope.Snapshot().Gauges()
	suite.Contains(gauges, "registered_hosts+")
	suite.Equal(float64(numAgents), gauges["registered_hosts+"].Value())
	suite.Contains(gauges, "cpus+")
	suite.Equal(float64(numAgents*_defaultResourceValue), gauges["cpus+"].Value())
	suite.Contains(gauges, "mem+")
	suite.Equal(float64(numAgents*_defaultResourceValue), gauges["mem+"].Value())
	suite.Contains(gauges, "disk+")
	suite.Equal(float64(numAgents*_defaultResourceValue), gauges["disk+"].Value())
	suite.Contains(gauges, "gpus+")
	suite.Equal(float64(numAgents*_defaultResourceValue), gauges["gpus+"].Value())
}

func TestHostMapTestSuite(t *testing.T) {
	suite.Run(t, new(HostMapTestSuite))
}
