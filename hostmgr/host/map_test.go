package host

import (
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	"code.uber.internal/infra/peloton/common"

	"code.uber.internal/infra/peloton/util"
	mock_mpb "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"
)

var (
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
				WithName(common.MesosCPU).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosMem).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosDisk).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosGPU).
				WithValue(resVal).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosCPU).
				WithValue(resVal).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build(),
			util.NewMesosResourceBuilder().
				WithName(common.MesosMem).
				WithValue(resVal).
				WithRevocable(&mesos.Resource_RevocableInfo{}).
				Build(),
		}
		getAgent := &mesos_master.Response_GetAgents_Agent{
			AgentInfo: &mesos.AgentInfo{
				Hostname:  &tmpID,
				Resources: resources,
			},
			TotalResources: resources,
		}
		response.Agents = append(response.Agents, getAgent)
	}

	return response
}

func (suite *HostMapTestSuite) TestRefresh() {
	defer suite.ctrl.Finish()

	loader := &Loader{
		OperatorClient:     suite.operatorClient,
		Scope:              suite.testScope,
		SlackResourceTypes: []string{common.MesosCPU},
	}

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().
			Return(nil, errors.New("unable to get agents")),
	)
	loader.Load(nil)

	numAgents := 2000
	response := makeAgentsResponse(numAgents)
	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(response, nil),
	)
	loader.Load(nil)

	m := GetAgentMap()
	suite.Len(m.RegisteredAgents, numAgents)

	id1 := "id-1"
	a1 := GetAgentInfo(id1)
	suite.NotEmpty(a1.Resources)
	id2 := "id-20000"
	a2 := GetAgentInfo(id2)
	suite.Nil(a2)

	gauges := suite.testScope.Snapshot().Gauges()
	suite.Contains(gauges, "registered_hosts+")
	suite.Equal(float64(numAgents), gauges["registered_hosts+"].Value())
	suite.Contains(gauges, "cpus+")
	suite.Equal(float64(numAgents*_defaultResourceValue), gauges["cpus+"].Value())
	suite.Contains(gauges, "cpus_revocable+")
	suite.Equal(float64(numAgents*_defaultResourceValue), gauges["cpus_revocable+"].Value())
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
