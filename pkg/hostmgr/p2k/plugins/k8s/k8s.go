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
	"fmt"

	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

const EventChanSize = 1000

// K8SManager implements the plugin for the Kubernetes cluster manager.
type K8SManager struct {
	// K8s client.
	kubeClient kubernetes.Interface

	// Internal K8S client structs that provide pod and node watch
	// functionality.
	informerFactory informers.SharedInformerFactory
	nodeInformer    coreinformers.NodeInformer
	podInformer     coreinformers.PodInformer

	// Pod events channel.
	podEventCh chan<- *scalar.PodEvent

	// Host events channel.
	hostEventCh chan<- *scalar.HostEvent

	// Lifecycle manager.
	lifecycle lifecycle.LifeCycle
}

// NewK8sManager returns a new instance of K8SManager
func NewK8sManager(
	configPath string,
	podEventCh chan<- *scalar.PodEvent,
	hostEventCh chan<- *scalar.HostEvent,
) (*K8SManager, error) {
	// Initialize k8s client.
	kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: configPath},
		&clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating kube config: %v", err)
	}

	kubeClient, err := kubernetes.NewForConfig(
		rest.AddUserAgent(kubeConfig, "peloton-scheduler"))
	if err != nil {
		return nil, fmt.Errorf("error creating kube client: %v", err)
	}

	return NewK8sManagerWithClient(kubeClient, podEventCh, hostEventCh), nil
}

// NewK8sManagerWithClient returns a new instance of K8SManager with given k8s
// client.
func NewK8sManagerWithClient(
	kubeClient kubernetes.Interface,
	podEventCh chan<- *scalar.PodEvent,
	hostEventCh chan<- *scalar.HostEvent,
) *K8SManager {
	// Initialize informers.
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	nodeInformer := informerFactory.Core().V1().Nodes()
	podInformer := informerFactory.Core().V1().Pods()

	return &K8SManager{
		kubeClient:      kubeClient,
		informerFactory: informerFactory,
		nodeInformer:    nodeInformer,
		podInformer:     podInformer,
		podEventCh:      podEventCh,
		hostEventCh:     hostEventCh,
		lifecycle:       lifecycle.NewLifeCycle(),
	}
}

// Start starts the k8s manager plugin
func (k *K8SManager) Start() {
	if !k.lifecycle.Start() {
		log.Warn("K8SManager is already started")
		return
	}

	// Add event callbacks to nodeInformer and podInformer.
	k.nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    k.addNode,
		UpdateFunc: k.updateNode,
		DeleteFunc: k.deleteNode,
	})
	k.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    k.addPod,
		UpdateFunc: k.updatePod,
		DeleteFunc: k.deletePod,
	})

	// Start informers.
	k.informerFactory.Start(k.lifecycle.StopCh())

	// Wait for all started informers cache were synced before scheduling.
	k.informerFactory.WaitForCacheSync(k.lifecycle.StopCh())
}

// Stop stops the K8SManager.
func (k *K8SManager) Stop() {
	if !k.lifecycle.Stop() {
		log.Warn("K8SManager already stopped")
		return
	}
	// Wait for drainer to be stopped.
	k.lifecycle.Wait()
	log.Info("K8SManager stopped")
}

// ReconcileHosts lists all nodes on the API server and converts them into
// host infos.
func (k *K8SManager) ReconcileHosts() ([]*scalar.HostInfo, error) {
	nodes, err := k.listNodes()
	if err != nil {
		return nil, err
	}

	hostInfos := make([]*scalar.HostInfo, len(nodes))
	for i, node := range nodes {
		evt, _ := scalar.BuildHostEventFromNode(node, scalar.AddHost)
		// TODO: catch and aggregate erros here
		hostInfos[i] = evt.GetHostInfo()
	}

	log.Info("reconcile hosts")
	return hostInfos, nil
}

// AckPodEvent is relevant to Mesos. For K8s for now, this is a noop.
// We could use some smarts here if we decide to write the resource version
// to DB after the event has been acknowledged by both JM and RM
func (k *K8SManager) AckPodEvent(ctx context.Context, event *scalar.PodEvent) {
}

// K8s Reconcile logic.

// use K8s node lister to get the current list of nodes from K8s API server.
func (k *K8SManager) listNodes() ([]*corev1.Node, error) {
	return k.nodeInformer.Lister().List(labels.Everything())
}

// K8s NodeInformer callbacks.

// NodeInformer add function.s
func (k *K8SManager) addNode(obj interface{}) {
	// Convert node event to host event.
	node := obj.(*corev1.Node)
	evt, err := scalar.BuildHostEventFromNode(node, scalar.AddHost)
	if err != nil {
		// Drop this error, reconcile will take care of this.
		log.WithFields(log.Fields{
			"node": node.Name,
		}).WithError(err).Error("error building add host event")
		return
	}
	log.WithFields(log.Fields{
		"node":  node,
		"event": evt.GetEventType(),
		"name":  evt.GetHostInfo().GetHostName(),
	}).Debug("add node event")
	k.hostEventCh <- evt
}

// NodeInformer update function.
func (k *K8SManager) updateNode(old interface{}, new interface{}) {
	node := new.(*corev1.Node)
	evt, err := scalar.BuildHostEventFromNode(node, scalar.UpdateHost)
	if err != nil {
		// Drop this error, reconcile will take care of this.
		log.WithFields(log.Fields{
			"node": node.Name,
		}).WithError(err).Error("error building update host event")
		return
	}

	log.WithFields(log.Fields{
		"event": evt.GetEventType(),
		"name":  evt.GetHostInfo().GetHostName(),
	}).Debug("update node event")
	k.hostEventCh <- evt
}

// NodeInformer delete function.
func (k *K8SManager) deleteNode(obj interface{}) {
	node := obj.(*corev1.Node)
	evt, err := scalar.BuildHostEventFromNode(node, scalar.DeleteHost)
	if err != nil {
		// Drop this error, reconcile will take care of this.
		log.WithFields(log.Fields{
			"node": node.Name,
		}).WithError(err).Warn("error building delete host event")
		return
	}
	log.WithFields(log.Fields{
		"event": evt.GetEventType(),
		"name":  evt.GetHostInfo().GetHostName(),
	}).Debug("delete node event")
	k.hostEventCh <- evt
}

// K8s pod events.

// PodInformer add function.
func (k *K8SManager) addPod(obj interface{}) {
	pod := obj.(*corev1.Pod)
	if pod.Spec.SchedulerName != common.PelotonRole {
		// TODO: Generate an alert.
		log.WithFields(log.Fields{
			"pod": pod,
		}).Debug("non-peloton pod")
		return
	}

	evt := scalar.BuildPodEventFromPod(pod, scalar.AddPod)
	log.WithFields(log.Fields{
		"pod": pod,
	}).Debug("add node event")
	k.podEventCh <- evt
}

// PodInformer update function.
func (k *K8SManager) updatePod(obj interface{}, obj2 interface{}) {
	pod := obj.(*corev1.Pod)
	if pod.Spec.SchedulerName != common.PelotonRole {
		// TODO: Generate an alert.
		log.WithFields(log.Fields{
			"pod": pod,
		}).Debug("non-peloton pod")
		return
	}

	evt := scalar.BuildPodEventFromPod(pod, scalar.UpdatePod)
	log.WithFields(log.Fields{
		"pod": pod,
	}).Debug("update node event")
	k.podEventCh <- evt
}

// PodInformer delete function.
func (k *K8SManager) deletePod(obj interface{}) {
	pod := obj.(*corev1.Pod)
	if pod.Spec.SchedulerName != common.PelotonRole {
		// TODO: Generate an alert.
		log.WithFields(log.Fields{
			"pod": pod,
		}).Debug("non-peloton pod")
		return
	}

	evt := scalar.BuildPodEventFromPod(pod, scalar.DeletePod)
	log.WithFields(log.Fields{
		"pod": pod,
	}).Debug("delete node event")
	k.podEventCh <- evt
}

// K8S API calls.

// LaunchPod creates a pod object, binds it to the node specified by hostname.
func (k *K8SManager) LaunchPod(
	podSpec *pbpod.PodSpec,
	podID, hostname string,
) error {
	// Convert v1alpha podSpec to k8s podSpec.
	pod := toK8SPodSpec(podSpec)

	pod.Spec.SchedulerName = common.PelotonRole

	// Bind the pod to the host.
	pod.Spec.NodeName = hostname

	// Overload pod.Name as pod ID. This needs to be done because jobmgr will
	// create a podID for its tracking purposes and store it as the key for
	// pod_runtime/task_runtime table. We need to associate that ID to the
	// pod in etcd. The best place to do this seems pod.Name.
	// Note that the UID in the object metadata associated with this pod is
	// system generated and is read only, so we cannot set it here.
	pod.Name = podID

	// Create the pod
	_, err := k.kubeClient.CoreV1().Pods("default").Create(pod)
	return err
}

// KillPod stops and deletes the given pod
func (k *K8SManager) KillPod(podID string) error {
	// There is no concept of "stopping" a pod in kubernetes (so nothing like
	// mesos Task Kill exists). So we need to treat this pod like a REST object
	// and just delete it from the API server. Special considerations need to be
	// made for getting the logs of terminal pods, out of scope for Peloton.
	return k.kubeClient.CoreV1().
		Pods("default").
		Delete(podID, &metav1.DeleteOptions{})
}
