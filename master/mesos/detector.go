/**
* This file is copied from mesos-uns-bridge/mesos/detector.go with modifications :
      1) refer to forked mesos-go dependencies
      2) added GetMasterLocation() which retries on empty MasterLocation
*/
package mesos

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/mesos-go/detector"
	_ "code.uber.internal/infra/peloton/mesos-go/detector/zoo" // To register zookeeper based plugin.
	mesos_v1 "mesos/v1"
)

const (
	zkPathPrefix                  = "zk://"
	getMesosMasterHostPortTimeout = 5 * time.Minute
	retrySleepTime                = 5 * time.Second
)

// MasterDetector is the interface for finding where is an active Mesos master.
// TODO : refactor to remove redundant logic
type MasterDetector interface {
	// MasterLocation returns ip and port for a Mesos master cached by MasterDetector
	MasterLocation() (string, int)
	// GetMasterLocation returns non empty ip and port for a Mesos master.
	GetMasterLocation() (string, error)
}

type zkDetector struct {
	mutex      sync.RWMutex
	masterIP   string
	masterPort int
}

// masterLocation returns ip and port for a Mesos master even if empty.
func (d *zkDetector) MasterLocation() (string, int) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return d.masterIP, d.masterPort
}

// GetMesosMasterHostPort returns mesos master host port from zk
func (d *zkDetector) GetMasterLocation() (string, error) {
	deadline := time.Now().Add(getMesosMasterHostPortTimeout)
	for time.Now().Before(deadline) {
		masterIP, masterPort := d.MasterLocation()
		if masterIP != "" && masterPort != 0 {
			mesosHostPort := fmt.Sprintf("%v:%v", masterIP, masterPort)
			return mesosHostPort, nil
		}
		log.Warn("No leading mesos master detected yet, sleep and retry")
		time.Sleep(retrySleepTime)
	}
	return "", fmt.Errorf("No leading master detected after %v", getMesosMasterHostPortTimeout)
}

// NewZKDetector creates a new MasterDetector which caches last detected leader.
func NewZKDetector(zkPath string) (MasterDetector, error) {
	if !strings.HasPrefix(zkPath, zkPathPrefix) {
		return nil, fmt.Errorf("zkPath must start with %s", zkPathPrefix)
	}

	d := zkDetector{}
	onMasterChangedCallback := detector.OnMasterChanged(func(master *mesos_v1.MasterInfo) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if master == nil || master.GetAddress() == nil {
			d.masterIP, d.masterPort = "", 0
		} else {
			d.masterIP, d.masterPort = master.GetAddress().GetIp(), int(master.GetAddress().GetPort())
		}
	})

	master, err := detector.New(zkPath)
	if err != nil {
		return nil, err
	}
	if err = master.Detect(onMasterChangedCallback); err != nil {
		return nil, err
	}
	// TODO: consider whether we need to Cancel (aka stop) this detector.
	return &d, nil
}
