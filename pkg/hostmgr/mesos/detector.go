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

/*
Package mesos is copied from mesos-uns-bridge/mesos/detector.go with modifications :
1) refer to forked mesos-go dependencies
*/
package mesos

import (
	"fmt"
	"strings"
	"sync"

	mesos "github.com/uber/peloton/.gen/mesos/v1"

	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector"
	_ "github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector/zoo" // To register zookeeper based plugin.
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/transport/mhttp"
)

const (
	zkPathPrefix = "zk://"
)

// MasterDetector is the interface for finding where is an active Mesos master.
type MasterDetector interface {
	mhttp.LeaderDetector
}

type zkDetector struct {
	sync.RWMutex

	masterIP   string
	masterPort int

	// Keep actual detector implementation wrapped so we can cancel it.
	m detector.Master
}

// HostPort implements mhttp.LeaderDetector and returns cached host port
// if has one.
func (d *zkDetector) HostPort() string {
	d.RLock()
	defer d.RUnlock()
	if d.masterIP == "" || d.masterPort == 0 {
		return ""
	}
	return fmt.Sprintf("%v:%v", d.masterIP, d.masterPort)
}

// OnMasterChanged implements `detector.MasterChanged.OnMasterChanged`.
// This is called whenever underlying detector detected leader change.
func (d *zkDetector) OnMasterChanged(masterInfo *mesos.MasterInfo) {
	d.Lock()
	defer d.Unlock()

	if masterInfo == nil || masterInfo.GetAddress() == nil {
		d.masterIP, d.masterPort = "", 0
	} else {
		d.masterIP, d.masterPort =
			masterInfo.GetAddress().GetIp(),
			int(masterInfo.GetAddress().GetPort())
	}
}

// NewZKDetector creates a new MasterDetector which caches last detected leader.
func NewZKDetector(zkPath string) (MasterDetector, error) {
	if !strings.HasPrefix(zkPath, zkPathPrefix) {
		return nil, fmt.Errorf(
			"zkPath must start with %s",
			zkPathPrefix)
	}

	master, err := detector.New(zkPath)
	if err != nil {
		return nil, err
	}

	d := &zkDetector{
		m: master,
	}

	if err = master.Detect(d); err != nil {
		return nil, err
	}

	// TODO: handle `Done()` from `master` so we know that underlying
	// detector accidentally finished.
	// TODO: consider whether we need to Cancel (aka stop) this detector.
	return d, nil
}
