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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclient "k8s.io/client-go/kubernetes/fake"
)

func TestLaunchAndKillPod(t *testing.T) {
	require := require.New(t)

	testPodName := "test_pod"
	testHostName := "test_host"
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	fakeClient := testclient.NewSimpleClientset()
	testManager := NewK8sManagerWithClient(
		fakeClient,
		podEventCh,
		hostEventCh,
	)
	testManager.Start()

	// Launch pod and verify.
	testPodSpec := newTestPelotonPodSpec(testPodName)
	err := testManager.LaunchPod(testPodSpec, testPodName, testHostName)
	require.NoError(err)

	returnedPod, err := fakeClient.CoreV1().Pods("default").Get(testPodName, metav1.GetOptions{})
	require.NoError(err)
	require.NotNil(returnedPod)
	require.Equal(testPodName, returnedPod.Name)

	// Kill pod and verify.
	err = testManager.KillPod(testPodName)
	require.NoError(err)

	returnedPod, err = fakeClient.CoreV1().Pods("default").Get(testPodName, metav1.GetOptions{})
	require.Error(err)
	require.Nil(returnedPod)
	_, ok := err.(*apierrors.StatusError)
	require.True(ok)
}

func TestPodEventHandlers(t *testing.T) {
	require := require.New(t)

	testPodName := "test_pod"
	testHostName := "test_host"
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	fakeClient := testclient.NewSimpleClientset()
	testManager := NewK8sManagerWithClient(
		fakeClient,
		podEventCh,
		hostEventCh,
	)
	testManager.Start()

	// Add pod via LaunchPod().
	testPodSpec := newTestPelotonPodSpec(testPodName)
	err := testManager.LaunchPod(testPodSpec, testPodName, testHostName)
	require.NoError(err)

	evt := <-podEventCh
	require.Equal(scalar.AddPod, evt.EventType)
	require.Equal(&peloton.PodID{Value: testPodName}, evt.Event.PodId)

	// Update pod with new phase.
	returnedPod, err := fakeClient.CoreV1().Pods("default").Get(testPodName, metav1.GetOptions{})
	require.NoError(err)
	require.NotNil(returnedPod)
	require.Equal(corev1.PodPhase(""), returnedPod.Status.Phase)
	returnedPod.Status.Phase = corev1.PodRunning
	_, err = fakeClient.CoreV1().Pods("default").UpdateStatus(returnedPod)
	require.NoError(err)

	evt = <-podEventCh
	require.Equal(scalar.UpdatePod, evt.EventType)
	require.Equal(&peloton.PodID{Value: testPodName}, evt.Event.PodId)
	require.Equal(corev1.PodRunning, returnedPod.Status.Phase)

	// Delete pod via KillPod().
	err = testManager.KillPod(testPodName)
	require.NoError(err)

	evt = <-podEventCh
	require.Equal(scalar.DeletePod, evt.EventType)
	require.Equal(&peloton.PodID{Value: testPodName}, evt.Event.PodId)

	// Verify channel is empty.
	select {
	case <-podEventCh:
	default:
	}
}

func TestHostEventHandlers(t *testing.T) {
	require := require.New(t)

	testHostName := "test_host"
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	fakeClient := testclient.NewSimpleClientset()
	testManager := NewK8sManagerWithClient(
		fakeClient,
		podEventCh,
		hostEventCh,
	)
	testManager.Start()

	// Add host.
	testNode := newTestK8sNode(testHostName)
	fakeClient.CoreV1().Nodes().Create(testNode)

	evt := <-hostEventCh
	require.Equal(scalar.AddHost, evt.GetEventType())
	require.Equal(testHostName, evt.GetHostInfo().GetHostName())

	// Update host allocatable resource.
	returnedNode, err := fakeClient.CoreV1().Nodes().Get(testHostName, metav1.GetOptions{})
	require.NoError(err)
	require.NotNil(returnedNode)
	returnedNode.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("16"),
		corev1.ResourceMemory: resource.MustParse("96Gi"),
	}
	_, err = fakeClient.CoreV1().Nodes().UpdateStatus(returnedNode)
	require.NoError(err)

	evt = <-hostEventCh
	require.Equal(scalar.UpdateHost, evt.GetEventType())
	require.Equal(testHostName, evt.GetHostInfo().GetHostName())

	// Delete host.
	err = fakeClient.CoreV1().Nodes().Delete(testHostName, &metav1.DeleteOptions{})
	require.NoError(err)

	evt = <-hostEventCh
	require.Equal(scalar.DeleteHost, evt.GetEventType())
	require.Equal(testHostName, evt.GetHostInfo().GetHostName())

	// Verify channel is empty.
	select {
	case <-hostEventCh:
	default:
	}
}
