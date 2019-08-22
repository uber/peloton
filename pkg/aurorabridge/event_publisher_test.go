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

package aurorabridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	jobmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	podmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	watchmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc/mocks"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/common/util"
	"go.uber.org/goleak"
	"go.uber.org/thriftrw/ptr"
)

type EventPublisherTestSuite struct {
	suite.Suite

	ctx         context.Context
	ctrl        *gomock.Controller
	kafkaURL    string
	jobClient   *jobmocks.MockJobServiceYARPCClient
	podClient   *podmocks.MockPodServiceYARPCClient
	watchClient *watchmocks.MockWatchServiceYARPCClient

	server     *httptest.Server
	httpClient *http.Client
	stream     *watchmocks.MockWatchServiceServiceWatchYARPCClient

	jobID   []string
	podID   []string
	podName []string

	eventPublisher EventPublisher
}

func (suite *EventPublisherTestSuite) SetupTest() {
	suite.ctx = context.Background()

	suite.server = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {

		suite.Equal("reliable", req.Header.Get("Producer-Type"))
		suite.Equal("application/vnd.kafka.binary.v1", req.Header.Get("Content-Type"))

		// validate each http request body has expected jobID, podID & podName
		body, err := ioutil.ReadAll(req.Body)
		suite.NoError(err)
		suite.True(contains(suite.jobID, string(body)))
		suite.True(contains(suite.podName, string(body)))
		suite.True(contains(suite.podID, string(body)))

		rw.Write([]byte(`OK`))
	}))

	suite.httpClient = suite.server.Client()

	suite.ctrl = gomock.NewController(suite.T())
	suite.jobClient = jobmocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.podClient = podmocks.NewMockPodServiceYARPCClient(suite.ctrl)
	suite.watchClient = watchmocks.NewMockWatchServiceYARPCClient(suite.ctrl)
	suite.stream = watchmocks.NewMockWatchServiceServiceWatchYARPCClient(suite.ctrl)
	suite.kafkaURL = suite.server.URL

	suite.eventPublisher = NewEventPublisher(
		suite.kafkaURL,
		suite.jobClient,
		suite.podClient,
		suite.watchClient,
		suite.httpClient,
		true,
	)
}

func (suite *EventPublisherTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestEventPublisher(t *testing.T) {
	suite.Run(t, &EventPublisherTestSuite{})
}

// Tests the scenario where aurora bridge leader is elected and then loses the leadership
func (suite *EventPublisherTestSuite) TestEventPublisher_AuroraBridgeLeaderStartStop() {
	defer func() {
		suite.server.Close()
		goleak.VerifyNoLeaks(suite.T())
	}()

	// Stop the event publisher
	suite.eventPublisher.Stop()
	time.Sleep(2 * time.Second)

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil)

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		AnyTimes()

	suite.stream.EXPECT().
		CloseSend().
		Return(nil)

	// Start event publisher
	suite.eventPublisher.Start()
	time.Sleep(2 * time.Second)

	// Stop the event publisher
	suite.eventPublisher.Stop()
	time.Sleep(2 * time.Second)
}

// Tests the scenario where job manager leader changes and new stream is created on
// current leader
func (suite *EventPublisherTestSuite) TestEventPublisher_JobManagerLeaderChange() {
	defer func() {
		suite.server.Close()
		goleak.VerifyNoLeaks(suite.T())
	}()

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil)

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		Times(5)

	suite.stream.EXPECT().
		Recv().
		Return(nil, io.EOF)

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil)

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		AnyTimes()

	suite.stream.EXPECT().
		CloseSend().
		Return(nil)

	// Start event publisher
	suite.eventPublisher.Start()

	// Sleep for 15 seconds to initiate watch stream
	// receive pods
	// job manager leader changes and receive stream gets an error
	// create new stream with current job manager leader
	// continue to receive more pods
	time.Sleep(15 * time.Second)

	// Stop event publisher
	suite.eventPublisher.Stop()
	time.Sleep(2 * time.Second)
}

// Tests the scenario where transient network error can lead to close watch stream
func (suite *EventPublisherTestSuite) TestEventPublisher_StreamError() {
	defer func() {
		suite.server.Close()
		goleak.VerifyNoLeaks(suite.T())
	}()

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil)

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		Times(5)

	suite.stream.EXPECT().
		Recv().
		Return(nil, errors.New("stream closed due to transiet network error"))

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil)

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		AnyTimes()

	suite.stream.EXPECT().
		CloseSend().
		Return(nil)

	// Start event publisher
	suite.eventPublisher.Start()

	// Sleep for 15 seconds to initiate watch stream
	// receive pods
	// job manager leader changes and receive stream gets an error
	// create new stream with current job manager leader
	// continue to receive more pods
	time.Sleep(15 * time.Second)

	// Stop event publisher
	suite.eventPublisher.Stop()
	time.Sleep(2 * time.Second)
}

func (suite *EventPublisherTestSuite) TestEventPublisher_GetTaskStateChangeErrors() {
	defer func() {
		suite.server.Close()
		goleak.VerifyNoLeaks(suite.T())
	}()

	jobID := &peloton.JobID{
		Value: "58f45b58-7eaf-459a-94cd-39526500525c",
	}
	podName := &peloton.PodName{
		Value: "58f45b58-7eaf-459a-94cd-39526500525c-0",
	}
	podID := &peloton.PodID{
		Value: "58f45b58-7eaf-459a-94cd-39526500525c-0-0",
	}

	pods := []*pod.PodSummary{
		{
			PodName: podName,
			Status: &pod.PodStatus{
				PodId: podID,
			},
		},
	}

	// Setup watch stream
	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil).
		AnyTimes()

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{
			Pods: pods,
		}, nil).
		Times(2)

	// Error on getting job info
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("unable to get job info"))

	// Error on getting podinfo
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				JobId: jobID,
				Name:  atop.NewJobName(fixture.AuroraJobKey()),
			},
		}, nil)

	suite.podClient.EXPECT().
		GetPod(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("unable to get pod info"))

	// Close watch stream
	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		AnyTimes()
	suite.stream.EXPECT().
		CloseSend().
		Return(nil)

	suite.eventPublisher.Start()
	time.Sleep(15 * time.Second)

	suite.eventPublisher.Stop()
	time.Sleep(2 * time.Second)
}

// Tests whether the published task on kafka is as expected
func (suite *EventPublisherTestSuite) TestEventPublisher_ReceivePods() {
	defer func() {
		suite.server.Close()
		goleak.VerifyNoLeaks(suite.T())
	}()

	host := "peloton-host-0"
	hostID := "6a2fe3f4-504c-48e9-b04f-9db7c02aa484-S0"

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), &watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				Labels: []*peloton.Label{
					common.BridgePodLabel,
				},
			},
		}).Return(suite.stream, nil)

	// generates 9 pod summaries
	pods := suite.generatePodSummary(3, 3)

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{
			Pods: pods,
		}, nil)

	for _, podSummary := range pods {
		jobID, _, _ := util.ParseTaskID(podSummary.GetPodName().GetValue())

		suite.jobClient.EXPECT().
			GetJob(gomock.Any(), gomock.Any()).
			Return(&statelesssvc.GetJobResponse{
				Summary: &stateless.JobSummary{
					JobId: &peloton.JobID{
						Value: jobID,
					},
					Name: atop.NewJobName(fixture.AuroraJobKey()),
				},
			}, nil)

		suite.podClient.EXPECT().
			GetPod(gomock.Any(), gomock.Any()).
			Return(&podsvc.GetPodResponse{
				Current: &pod.PodInfo{
					Spec: &pod.PodSpec{
						PodName:    podSummary.GetPodName(),
						Labels:     []*peloton.Label{},
						Containers: []*pod.ContainerSpec{{}},
					},
					Status: &pod.PodStatus{
						PodId:  podSummary.GetStatus().GetPodId(),
						Host:   host,
						State:  pod.PodState_POD_STATE_RUNNING,
						HostId: hostID,
					},
				},
			}, nil)
	}

	suite.stream.EXPECT().
		Recv().
		Return(&watchsvc.WatchResponse{}, nil).
		AnyTimes()
	suite.stream.EXPECT().
		CloseSend().
		Return(nil)

	suite.eventPublisher.Start()
	time.Sleep(15 * time.Second)

	suite.eventPublisher.Stop()
	time.Sleep(2 * time.Second)
}

// generatesPodSummaries for provided job count and number of pods per job
func (suite *EventPublisherTestSuite) generatePodSummary(jobCount, podCount int) []*pod.PodSummary {
	pods := []*pod.PodSummary{}
	for i := 0; i < jobCount; i++ {

		jobID := &peloton.JobID{
			Value: uuid.New(),
		}
		suite.jobID = append(suite.jobID, jobID.GetValue())

		for j := 0; j < podCount; j++ {

			podName := &peloton.PodName{
				Value: fmt.Sprintf("%s-%d", jobID.GetValue(), i),
			}
			suite.podName = append(suite.podName, podName.GetValue())

			podID := &peloton.PodID{
				Value: fmt.Sprintf("%s-%d", podName.GetValue(), rand.Intn(10)),
			}
			suite.podID = append(suite.podID, podID.GetValue())

			podSummary := &pod.PodSummary{
				PodName: podName,
				Status: &pod.PodStatus{
					PodId: podID,
					State: pod.PodState_POD_STATE_RUNNING,
				},
			}

			pods = append(pods, podSummary)
		}
	}

	return pods
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if strings.Contains(e, a) {
			return true
		}
	}
	return false
}

// TestModifyTaskStateChangeJSON tests modifyTaskStateChangeJSON util function
func TestModifyTaskStateChangeJSON(t *testing.T) {
	dockerContainer := &api.Container{
		Docker: &api.DockerContainer{
			Image: ptr.String("image"),
			Parameters: []*api.DockerParameter{
				{
					Name:  ptr.String("name"),
					Value: ptr.String("value"),
				},
			},
		},
	}
	stateChange1 := &taskStateChange{
		Task: &api.ScheduledTask{
			AssignedTask: &api.AssignedTask{
				Task: &api.TaskConfig{
					Container: dockerContainer,
				},
			},
		},
	}
	stateChange2 := &taskStateChange{
		Task: &api.ScheduledTask{
			AssignedTask: &api.AssignedTask{
				Task: &api.TaskConfig{
					IsService: ptr.Bool(true),
				},
			},
		},
	}

	// Expect "container" object to be modified
	msg1, err := json.Marshal(stateChange1)
	assert.NoError(t, err)
	assert.Equal(t, `{"task":{"assignedTask":{"task":{"container":{"docker":{"image":"image","parameters":[{"name":"name","value":"value"}]}}}}}}`, string(msg1))

	fmsg1, err := modifyTaskStateChangeJSON(msg1)
	assert.NoError(t, err)
	assert.Equal(t, `{"task":{"assignedTask":{"task":{"container":{"setField":"DOCKER","value":{"image":"image","parameters":[{"name":"name","value":"value"}]}}}}}}`, string(fmsg1))

	// Since container does not exist, expect no change in json string
	msg2, err := json.Marshal(stateChange2)
	assert.NoError(t, err)
	assert.Equal(t, `{"task":{"assignedTask":{"task":{"isService":true}}}}`, string(msg2))

	fmsg2, err := modifyTaskStateChangeJSON(msg2)
	assert.NoError(t, err)
	assert.Equal(t, `{"task":{"assignedTask":{"task":{"isService":true}}}}`, string(fmsg2))
}
