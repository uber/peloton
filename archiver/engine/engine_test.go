package engine

import (
	"context"

	job_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/job/mocks"
	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"
	"code.uber.internal/infra/peloton/archiver"
	"code.uber.internal/infra/peloton/cli"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type archiverEngineTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockJob     *job_mocks.MockJobManagerYARPCClient
	mockRespool *respool_mocks.MockResourceManagerYARPCClient
	ctx         context.Context
}

func (suite *archiverEngineTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockJob = job_mocks.NewMockJobManagerYARPCClient(suite.mockCtrl)
	suite.mockRespool = respool_mocks.NewMockResourceManagerYARPCClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *archiverEngineTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *archiverEngineTestSuite) TestEngineStartStop() {
	m := NewMetrics(tally.NoopScope)
	cfg := archiver.Config{}
	// TODO (adityacb): API to set internals of cli.Client
	// instead of just calling cli.New(). That way we can write more
	// interesting tests once Archiver code is ready.
	c := cli.Client{
		Debug: false,
	}
	e := &engine{
		client:  &c,
		config:  cfg,
		metrics: m,
	}
	e.Start()
	e.Stop()
}
