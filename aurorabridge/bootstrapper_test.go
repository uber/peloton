package aurorabridge

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool/svc/mocks"
)

type BootstrapperTestSuite struct {
	suite.Suite

	ctrl          *gomock.Controller
	respoolClient *mocks.MockResourcePoolServiceYARPCClient

	config BootstrapConfig

	bootstrapper *Bootstrapper
}

func (suite *BootstrapperTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.respoolClient = mocks.NewMockResourcePoolServiceYARPCClient(suite.ctrl)

	suite.config = BootstrapConfig{
		RespoolPath: "/AuroraBridge",
		DefaultRespoolSpec: DefaultRespoolSpec{
			OwningTeam:  "some-team",
			LDAPGroups:  []string{"some-group"},
			Description: "some description",
			Resources: []*respool.ResourceSpec{{
				Kind:        "cpu",
				Reservation: 12,
				Limit:       12,
				Share:       1,
			}},
			Policy: respool.SchedulingPolicy_SCHEDULING_POLICY_PRIORITY_FIFO,
			ControllerLimit: &respool.ControllerLimit{
				MaxPercent: 30,
			},
		},
	}

	suite.bootstrapper = NewBootstrapper(
		suite.config,
		suite.respoolClient,
	)
}

func (suite *BootstrapperTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestBootstrapper(t *testing.T) {
	suite.Run(t, &BootstrapperTestSuite{})
}

func (suite *BootstrapperTestSuite) TestBootstrapRespool_ExistingPool() {
	id := &peloton.ResourcePoolID{Value: "bridge-id"}

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &svc.LookupResourcePoolIDRequest{
			Path: &respool.ResourcePoolPath{Value: suite.config.RespoolPath},
		}).
		Return(&svc.LookupResourcePoolIDResponse{
			RespoolId: id,
		}, nil)

	result, err := suite.bootstrapper.BootstrapRespool()
	suite.NoError(err)
	suite.Equal(id, result)
}

func (suite *BootstrapperTestSuite) TestBootstrapRespool_NewPoolUsesDefaults() {
	rootID := &peloton.ResourcePoolID{Value: "root-id"}
	id := &peloton.ResourcePoolID{Value: "bridge-id"}

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &svc.LookupResourcePoolIDRequest{
			Path: &respool.ResourcePoolPath{Value: suite.config.RespoolPath},
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &svc.LookupResourcePoolIDRequest{
			Path: &respool.ResourcePoolPath{Value: "/"},
		}).
		Return(&svc.LookupResourcePoolIDResponse{
			RespoolId: rootID,
		}, nil)

	suite.respoolClient.EXPECT().
		CreateResourcePool(gomock.Any(), &svc.CreateResourcePoolRequest{
			Spec: &respool.ResourcePoolSpec{
				Name:            "AuroraBridge",
				OwningTeam:      suite.config.DefaultRespoolSpec.OwningTeam,
				LdapGroups:      suite.config.DefaultRespoolSpec.LDAPGroups,
				Description:     suite.config.DefaultRespoolSpec.Description,
				Resources:       suite.config.DefaultRespoolSpec.Resources,
				Parent:          rootID,
				Policy:          suite.config.DefaultRespoolSpec.Policy,
				ControllerLimit: suite.config.DefaultRespoolSpec.ControllerLimit,
				SlackLimit:      suite.config.DefaultRespoolSpec.SlackLimit,
			},
		}).
		Return(&svc.CreateResourcePoolResponse{
			RespoolId: id,
		}, nil)

	result, err := suite.bootstrapper.BootstrapRespool()
	suite.NoError(err)
	suite.Equal(id, result)
}
