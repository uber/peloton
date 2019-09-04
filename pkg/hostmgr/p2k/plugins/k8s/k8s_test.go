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

package k8s

import (
	"context"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"
)

type K8SManagerTestSuite struct {
	suite.Suite

	podEventCh     chan *scalar.PodEvent
	hostEventCh    chan *scalar.HostEvent
	testKubeClient *testclient.Clientset
	testManager    *K8SManager
}

func (suite *K8SManagerTestSuite) SetupTest() {
	suite.podEventCh = make(chan *scalar.PodEvent, 1000)
	suite.hostEventCh = make(chan *scalar.HostEvent, 1000)
	suite.testKubeClient = testclient.NewSimpleClientset()
	suite.testManager = newK8sManagerWithClient(
		suite.testKubeClient,
		suite.podEventCh,
		suite.hostEventCh,
	)
}

func TestK8SManagerTestSuite(t *testing.T) {
	suite.Run(t, &K8SManagerTestSuite{})
}

func (suite *K8SManagerTestSuite) TestLaunchAndKillPod() {
	testPodName := "test_pod"
	testHostName := "test_host"

	suite.testManager.Start()

	// Launch pod and verify.
	testPodSpec := newTestPelotonPodSpec(testPodName)
	launched, err := suite.testManager.LaunchPods(
		context.Background(),
		[]*models.LaunchablePod{
			{PodId: &peloton.PodID{Value: testPodName}, Spec: testPodSpec},
		},
		testHostName,
	)
	suite.NoError(err)
	suite.Equal(1, len(launched))

	returnedPod, err := suite.
		testKubeClient.
		CoreV1().
		Pods("default").
		Get(testPodName, metav1.GetOptions{})
	suite.NoError(err)
	suite.NotNil(returnedPod)
	suite.Equal(testPodName, returnedPod.Name)

	// Kill pod and verify.
	err = suite.testManager.KillPod(context.Background(), testPodName)
	suite.NoError(err)

	returnedPod, err = suite.
		testKubeClient.
		CoreV1().
		Pods("default").
		Get(testPodName, metav1.GetOptions{})
	suite.Error(err)
	suite.Nil(returnedPod)
	_, ok := err.(*apierrors.StatusError)
	suite.True(ok)
}

func (suite *K8SManagerTestSuite) TestPodEventHandlers() {
	testPodName := "test_pod"
	testHostName := "test_host"

	suite.testManager.Start()

	// Add pod via LaunchPods().
	testPodSpec := newTestPelotonPodSpec(testPodName)
	launched, err := suite.testManager.LaunchPods(
		context.Background(),
		[]*models.LaunchablePod{
			{PodId: &peloton.PodID{Value: testPodName}, Spec: testPodSpec},
		},
		testHostName,
	)
	suite.NoError(err)
	suite.Equal(1, len(launched))

	evt := <-suite.podEventCh
	suite.Equal(scalar.AddPod, evt.EventType)
	suite.Equal(&peloton.PodID{Value: testPodName}, evt.Event.PodId)

	// Update pod with new phase.
	returnedPod, err := suite.
		testKubeClient.
		CoreV1().
		Pods("default").
		Get(testPodName, metav1.GetOptions{})
	suite.NoError(err)
	suite.NotNil(returnedPod)
	suite.Equal(corev1.PodPhase(""), returnedPod.Status.Phase)
	returnedPod.Status.Phase = corev1.PodRunning
	_, err = suite.testKubeClient.CoreV1().Pods("default").UpdateStatus(returnedPod)
	suite.NoError(err)

	evt = <-suite.podEventCh
	suite.Equal(scalar.UpdatePod, evt.EventType)
	suite.Equal(&peloton.PodID{Value: testPodName}, evt.Event.PodId)
	suite.Equal(corev1.PodRunning, returnedPod.Status.Phase)

	// Delete pod via KillPod().
	err = suite.testManager.KillPod(context.Background(), testPodName)
	suite.NoError(err)

	evt = <-suite.podEventCh
	suite.Equal(scalar.DeletePod, evt.EventType)
	suite.Equal(&peloton.PodID{Value: testPodName}, evt.Event.PodId)

	// Verify channel is empty.
	select {
	case <-suite.podEventCh:
	default:
	}
}

func (suite *K8SManagerTestSuite) TestHostEventHandlers() {
	testHostName := "test_host"
	suite.testManager.Start()

	// Add host.
	testNode := newTestK8sNode(testHostName)
	suite.testKubeClient.CoreV1().Nodes().Create(testNode)

	evt := <-suite.hostEventCh
	suite.Equal(scalar.AddHost, evt.GetEventType())
	suite.Equal(testHostName, evt.GetHostInfo().GetHostName())

	// Update host allocatable resource.
	returnedNode, err := suite.testKubeClient.CoreV1().Nodes().Get(testHostName, metav1.GetOptions{})
	suite.NoError(err)
	suite.NotNil(returnedNode)
	returnedNode.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("16"),
		corev1.ResourceMemory: resource.MustParse("96Gi"),
	}
	_, err = suite.testKubeClient.CoreV1().Nodes().UpdateStatus(returnedNode)
	suite.NoError(err)

	evt = <-suite.hostEventCh
	suite.Equal(scalar.UpdateHostSpec, evt.GetEventType())
	suite.Equal(testHostName, evt.GetHostInfo().GetHostName())

	// Delete host.
	err = suite.testKubeClient.CoreV1().Nodes().Delete(testHostName, &metav1.DeleteOptions{})
	suite.NoError(err)

	evt = <-suite.hostEventCh
	suite.Equal(scalar.DeleteHost, evt.GetEventType())
	suite.Equal(testHostName, evt.GetHostInfo().GetHostName())

	// Verify channel is empty.
	select {
	case <-suite.hostEventCh:
	default:
	}
}
