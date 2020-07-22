/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zoo

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector"
)

const (
	// prefix for nodes listed at the ZK URL path
	nodePrefix                    = "info_"
	nodeJSONPrefix                = "json.info_"
	defaultMinDetectorCyclePeriod = 1 * time.Second
)

type (
	ZKInterface interface {
		Stopped() <-chan struct{}
		Stop()
		Data(string) ([]byte, error)
		WatchChildren(string) (string, <-chan []string, <-chan error)
	}

	infoCodec func(path, node string) (*mesos.MainInfo, error)

	// Detector uses ZooKeeper to detect new leading main.
	MainDetector struct {
		// detection should not signal main change listeners more frequently than this
		cancel func()
		client ZKInterface
		done   chan struct{}
		// latch: only install, at most, one ignoreChanged listener; see MainDetector.Detect
		ignoreInstalled        int32
		leaderNode             string
		minDetectorCyclePeriod time.Duration

		// guard against concurrent invocations of bootstrapFunc
		bootstrapLock sync.RWMutex
		bootstrapFunc func(ZKInterface, <-chan struct{}) (ZKInterface, error) // for one-time zk client initiation
	}
)

// reasonable default for a noop change listener
var ignoreChanged = detector.OnMainChanged(func(*mesos.MainInfo) {})

// MinCyclePeriod is a functional option that determines the highest frequency of main change notifications
func MinCyclePeriod(d time.Duration) detector.Option {
	return func(di interface{}) detector.Option {
		md := di.(*MainDetector)
		old := md.minDetectorCyclePeriod
		md.minDetectorCyclePeriod = d
		return MinCyclePeriod(old)
	}
}

func Bootstrap(f func(ZKInterface, <-chan struct{}) (ZKInterface, error)) detector.Option {
	return func(di interface{}) detector.Option {
		md := di.(*MainDetector)
		old := md.bootstrapFunc
		md.bootstrapFunc = f
		return Bootstrap(old)
	}
}

// Internal constructor function
func NewMainDetector(zkurls string, options ...detector.Option) (*MainDetector, error) {
	zkHosts, zkPath, err := parseZk(zkurls)
	if err != nil {
		log.Fatalf("Failed to parse url , err=%v", err)
		return nil, err
	}

	detector := &MainDetector{
		minDetectorCyclePeriod: defaultMinDetectorCyclePeriod,
		done:                   make(chan struct{}),
		cancel:                 func() {},
	}

	detector.bootstrapFunc = func(client ZKInterface, _ <-chan struct{}) (ZKInterface, error) {
		if client == nil {
			return connect2(zkHosts, zkPath)
		}
		return client, nil
	}

	// apply options last so that they can override default behavior
	for _, opt := range options {
		opt(detector)
	}

	log.Infof("Created new detector to watch : %v , %v", zkHosts, zkPath)
	return detector, nil
}

func parseZk(zkurls string) ([]string, string, error) {
	u, err := url.Parse(zkurls)
	if err != nil {
		log.Infof("failed to parse url: %v", err)
		return nil, "", err
	}
	if u.Scheme != "zk" {
		return nil, "", fmt.Errorf("invalid url scheme for zk url: '%v'", u.Scheme)
	}
	return strings.Split(u.Host, ","), u.Path, nil
}

// returns a chan that, when closed, indicates termination of the detector
func (md *MainDetector) Done() <-chan struct{} {
	return md.done
}

func (md *MainDetector) Cancel() {
	md.bootstrapLock.RLock()
	defer md.bootstrapLock.RUnlock()
	md.cancel()
}

func (md *MainDetector) childrenChanged(path string, list []string, obs detector.MainChanged) {
	md.notifyMainChanged(path, list, obs)
	md.notifyAllMains(path, list, obs)
}

func (md *MainDetector) notifyMainChanged(path string, list []string, obs detector.MainChanged) {
	// mesos v0.24 writes JSON only, v0.23 writes json and protobuf, v0.22 and prior only write protobuf
	topNode, codec := md.selectTopNode(list)
	if md.leaderNode == topNode {
		log.Infof("ignoring children-changed event, leader has not changed: %v", path)
		return
	}

	log.Infof("changing leader node from %q -> %q", md.leaderNode, topNode)
	md.leaderNode = topNode

	var mainInfo *mesos.MainInfo
	if md.leaderNode != "" {
		var err error
		if mainInfo, err = codec(path, topNode); err != nil {
			log.Error(err.Error())
		}
	}
	log.Infof("detected main info: %+v", mainInfo)
	logPanic(func() { obs.OnMainChanged(mainInfo) })
}

// logPanic safely executes the given func, recovering from and logging a panic if one occurs.
func logPanic(f func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("recovered from client panic: %v", r)
		}
	}()
	f()
}

func (md *MainDetector) pullMainInfo(path, node string) (*mesos.MainInfo, error) {
	data, err := md.client.Data(fmt.Sprintf("%s/%s", path, node))
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve leader data: %v", err)
	}

	mainInfo := &mesos.MainInfo{}
	err = proto.Unmarshal(data, mainInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf MainInfo data from zookeeper: %v", err)
	}
	return mainInfo, nil
}

func (md *MainDetector) pullMainJsonInfo(path, node string) (*mesos.MainInfo, error) {
	data, err := md.client.Data(fmt.Sprintf("%s/%s", path, node))
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve leader data: %v", err)
	}

	mainInfo := &mesos.MainInfo{}
	err = json.Unmarshal(data, mainInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json MainInfo data from zookeeper: %v", err)
	}
	return mainInfo, nil
}

func (md *MainDetector) notifyAllMains(path string, list []string, obs detector.MainChanged) {
	all, ok := obs.(detector.AllMains)
	if !ok {
		// not interested in entire main list
		return
	}

	// mesos v0.24 writes JSON only, v0.23 writes json and protobuf, v0.22 and prior only write protobuf
	mains := map[string]*mesos.MainInfo{}
	tryStore := func(node string, codec infoCodec) {
		info, err := codec(path, node)
		if err != nil {
			log.Error(err.Error())
		} else {
			mains[info.GetId()] = info
		}
	}
	for _, node := range list {
		// compare https://github.com/apache/mesos/blob/0.23.0/src/master/detector.cpp#L437
		if strings.HasPrefix(node, nodePrefix) {
			tryStore(node, md.pullMainInfo)
		} else if strings.HasPrefix(node, nodeJSONPrefix) {
			tryStore(node, md.pullMainJsonInfo)
		} else {
			continue
		}
	}
	mainList := make([]*mesos.MainInfo, 0, len(mains))
	for _, v := range mains {
		mainList = append(mainList, v)
	}

	log.Infof("notifying of main membership change: %+v", mainList)
	logPanic(func() { all.UpdatedMains(mainList) })
}

func (md *MainDetector) callBootstrap() (e error) {
	log.Info("invoking detector boostrap")
	md.bootstrapLock.Lock()
	defer md.bootstrapLock.Unlock()

	clientConfigured := md.client != nil

	md.client, e = md.bootstrapFunc(md.client, md.done)
	if e == nil && !clientConfigured && md.client != nil {
		// chain the lifetime of this detector to that of the newly created client impl
		client := md.client
		md.cancel = client.Stop
		go func() {
			defer close(md.done)
			<-client.Stopped()
		}()
	}
	return
}

// the first call to Detect will kickstart a connection to zookeeper. a nil change listener may
// be spec'd, result of which is a detector that will still listen for main changes and record
// leaderhip changes internally but no listener would be notified. Detect may be called more than
// once, and each time the spec'd listener will be added to the list of those receiving notifications.
func (md *MainDetector) Detect(f detector.MainChanged) (err error) {
	// kickstart zk client connectivity
	if err := md.callBootstrap(); err != nil {
		log.Infof("failed to execute bootstrap function, err=%v", err.Error())
		return err
	}

	if f == nil {
		// only ever install, at most, one ignoreChanged listener. multiple instances of it
		// just consume resources and generate misleading log messages.
		if !atomic.CompareAndSwapInt32(&md.ignoreInstalled, 0, 1) {
			log.Info("ignoreChanged listener already installed")
			return
		}
		f = ignoreChanged
	}

	log.Info("spawning detect()")
	go md.detect(f)
	return nil
}

func (md *MainDetector) detect(f detector.MainChanged) {
	log.Infof("detecting children at %v", CurrentPath)
detectLoop:
	for {
		select {
		case <-md.Done():
			return
		default:
		}
		log.Infof("watching children at %v", CurrentPath)
		path, childrenCh, errCh := md.client.WatchChildren(CurrentPath)
		rewatch := false
		for {
			started := time.Now()
			select {
			case children := <-childrenCh:
				md.childrenChanged(path, children, f)
			case err, ok := <-errCh:
				// check for a tie first (required for predictability (tests)); the downside of
				// doing this is that a listener might get two callbacks back-to-back ("new leader",
				// followed by "no leader").
				select {
				case children := <-childrenCh:
					md.childrenChanged(path, children, f)
				default:
				}
				if ok {
					log.Infof("child watch ended with error, main lost; error was: %v", err.Error())
				} else {
					// detector shutdown likely...
					log.Info("child watch ended, main lost")
				}
				select {
				case <-md.Done():
					return
				default:
					if md.leaderNode != "" {
						log.Infof("changing leader node from %v -> \"\"", md.leaderNode)
						md.leaderNode = ""
						f.OnMainChanged(nil)
					}
				}
				rewatch = true
			}
			// rate-limit main changes
			if elapsed := time.Since(started); elapsed > 0 {
				sleep := md.minDetectorCyclePeriod - elapsed
				log.Infof("resting %v before next detection cycle", sleep)
				select {
				case <-md.Done():
					return
				case <-time.After(sleep): // noop
				}
			}
			if rewatch {
				continue detectLoop
			}
		}
	}
}

func (md *MainDetector) selectTopNode(list []string) (topNode string, codec infoCodec) {
	// mesos v0.24 writes JSON only, v0.23 writes json and protobuf, v0.22 and prior only write protobuf
	topNode = selectTopNodePrefix(list, nodeJSONPrefix)
	codec = md.pullMainJsonInfo
	if topNode == "" {
		topNode = selectTopNodePrefix(list, nodePrefix)
		codec = md.pullMainInfo

		if topNode != "" {
			log.Warnf("Leading main is using a Protobuf binary format when registering "+
				"with Zookeeper (%s): this will be deprecated as of Mesos 0.24 (see MESOS-2340).",
				topNode)
		}
	}
	return
}

func selectTopNodePrefix(list []string, pre string) (node string) {
	var leaderSeq uint64 = math.MaxUint64

	for _, v := range list {
		if !strings.HasPrefix(v, pre) {
			continue // only care about participants
		}
		seqStr := strings.TrimPrefix(v, pre)
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			log.Warnf("unexpected zk node format '%s': %v", seqStr, err)
			continue
		}
		if seq < leaderSeq {
			leaderSeq = seq
			node = v
		}
	}

	if node == "" {
		log.Info("No top node found.")
	} else {
		log.Infof("Top node selected: '%s'", node)
	}
	return node
}
