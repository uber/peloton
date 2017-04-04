/*
Package mesos is copied from mesos-uns-bridge/mesos/detector.go with modifications :
1) refer to forked mesos-go dependencies
*/
package mesos

import (
	"fmt"
	"strings"
	"sync"

	mesos "mesos/v1"

	"code.uber.internal/infra/peloton/mesos-go/detector"
	_ "code.uber.internal/infra/peloton/mesos-go/detector/zoo" // To register zookeeper based plugin.
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
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
