package mpb

import (
	"bytes"
	"io/ioutil"
	"testing"

	mesos "mesos/v1"
	mesos_master "mesos/v1/master"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	transport_mocks "go.uber.org/yarpc/transport/transporttest"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

type masterOperatorClientTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	mockClientCfg   *transport_mocks.MockClientConfig
	defaultEncoding string
}

func (suite *masterOperatorClientTestSuite) SetupTest() {
	log.Debug("setup")
	ctrl := gomock.NewController(suite.T())
	suite.ctrl = ctrl
	mockClientCfg := transport_mocks.NewMockClientConfig(suite.ctrl)
	suite.mockClientCfg = mockClientCfg
	suite.defaultEncoding = ContentTypeProtobuf
}

func (suite *masterOperatorClientTestSuite) TearDownTest() {
	log.Debug("tear down")
	if suite.ctrl != nil {
		suite.ctrl.Finish()
	}
}

func mesosResource(name string, value float64) *mesos.Resource {
	scalerType := mesos.Value_SCALAR

	return &mesos.Resource{
		Name: &name,
		Type: &scalerType,
		Scalar: &mesos.Value_Scalar{
			Value: &value,
		},
	}
}

func mesosRole(
	name string,
	weight float64,
	resources []*mesos.Resource,
	frameworkIDs []*mesos.FrameworkID) *mesos.Role {

	return &mesos.Role{
		Name:       &name,
		Weight:     &weight,
		Resources:  resources,
		Frameworks: frameworkIDs,
	}
}

func (suite *masterOperatorClientTestSuite) TestMasterOperatorClient_AllocatedResources() {

	mockCaller := "testCall"
	mockSvc := "testSvc"
	mockValidValue := new(string)
	*mockValidValue = uuid.NIL.String()
	mockNotExistValue := new(string)
	*mockNotExistValue = "do_not_exist"

	mockValidFrameWorkID := &mesos.FrameworkID{
		Value: mockValidValue,
	}

	mockNotExistFrameWorkID := &mesos.FrameworkID{
		Value: mockNotExistValue,
	}

	tests := []struct {
		call        bool
		frameworkID string
		encoding    string
		callResp    proto.Message
		headers     yarpc.Headers
		errMsg      string
		callErr     bool
	}{
		{
			call:        false,
			frameworkID: *mockValidValue,
			encoding:    "unknown",
			errMsg:      "failed to marshal Call_MasterOperator call: Unsupported contentType unknown",
		},
		{
			call:        false,
			frameworkID: "",
			encoding:    suite.defaultEncoding,
			errMsg:      "frameworkID cannot be empty",
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			errMsg:      "error making call Call_MasterOperator: connection error",
			callErr:     true,
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			callResp: &mesos_master.Response{
				GetRoles: &mesos_master.Response_GetRoles{
					Roles: []*mesos.Role{
						mesosRole(
							"peloton",
							1,
							[]*mesos.Resource{
								mesosResource(
									"cpus",
									200,
								),
							},
							[]*mesos.FrameworkID{
								mockValidFrameWorkID,
							},
						),
					},
				},
			},
			headers: yarpc.NewHeaders().With("a", "b"),
			errMsg:  "",
			callErr: false,
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			callResp: &mesos_master.Response{
				GetRoles: &mesos_master.Response_GetRoles{
					Roles: []*mesos.Role{
						mesosRole(
							"peloton",
							1,
							[]*mesos.Resource{
								mesosResource(
									"cpus",
									200,
								),
							},
							[]*mesos.FrameworkID{
								mockNotExistFrameWorkID,
							},
						),
					},
				},
			},
			headers: yarpc.NewHeaders().With("a", "b"),
			errMsg:  "no resources configured",
			callErr: false,
		},
		{
			call:        true,
			frameworkID: *mockValidValue,
			encoding:    suite.defaultEncoding,
			callResp: &mesos_master.Response{
				GetRoles: nil,
			},
			headers: yarpc.NewHeaders().With("a", "b"),
			errMsg:  "no resources fetched",
			callErr: false,
		},
	}

	for _, tt := range tests {

		// Check Yarpc call is needed
		if tt.call {
			mockUnaryOutbound := transport_mocks.NewMockUnaryOutbound(suite.ctrl)
			// Set expectations
			var err error
			var response *transport.Response

			if tt.callErr {
				err = errors.New("connection error")
			} else {

				wireData, err := proto.Marshal(tt.callResp)
				suite.NoError(err)

				response = &transport.Response{
					Body: ioutil.NopCloser(
						bytes.NewReader(wireData),
					),
					Headers: transport.Headers(tt.headers),
				}
			}

			gomock.InOrder(
				suite.mockClientCfg.EXPECT().Caller().Return(mockCaller),
				suite.mockClientCfg.EXPECT().Service().Return(mockSvc),
				suite.mockClientCfg.EXPECT().GetUnaryOutbound().Return(
					mockUnaryOutbound,
				),

				mockUnaryOutbound.EXPECT().Call(
					gomock.Any(),
					gomock.Any(),
				).Return(
					response,
					err,
				),
			)
		}

		mOClient := NewMasterOperatorClient(suite.mockClientCfg, tt.encoding)
		resources, err := mOClient.AllocatedResources(tt.frameworkID)

		if tt.errMsg != "" {
			suite.EqualError(err, tt.errMsg)
			suite.Nil(resources)
		} else {
			suite.NoError(err)
			suite.NotNil(resources)
		}
	}
}

func TestMasterOperatorClientTestSuite(t *testing.T) {
	suite.Run(t, new(masterOperatorClientTestSuite))
}
