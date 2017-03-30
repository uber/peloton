/*
Package mesos is copied from mesos-uns-bridge/mesos/detector.go with modifications :
1) refer to forked mesos-go dependencies
2) added GetMasterLocation() which retries on empty MasterLocation
*/
package mesos

import (
	"fmt"
	"strings"
	"sync"
	"time"

	mesos "mesos/v1"

	log "github.com/Sirupsen/logrus"

	"code.uber.internal/infra/peloton/mesos-go/detector"
	_ "code.uber.internal/infra/peloton/mesos-go/detector/zoo" // To register zookeeper based plugin.
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
	sync.RWMutex

	masterIP   string
	masterPort int

	// Keep actual detector implementation wrapped so we can cancel it.
	m detector.Master
}

// masterLocation returns ip and port for a Mesos master even if empty.
func (d *zkDetector) MasterLocation() (string, int) {
	d.RLock()
	defer d.RUnlock()
	return d.masterIP, d.masterPort
}

// GetMesosMasterHostPort returns mesos master host port from zk
func (d *zkDetector) GetMasterLocation() (string, error) {
	deadline := time.Now().Add(getMesosMasterHostPortTimeout)
	for time.Now().Before(deadline) {
		ip, port := d.MasterLocation()
		if ip != "" && port != 0 {
			mesosHostPort := fmt.Sprintf("%v:%v", ip, port)
			return mesosHostPort, nil
		}
		log.Warn("No leading mesos master detected yet, sleep and retry")
		time.Sleep(retrySleepTime)
	}
	return "", fmt.Errorf(
		"No leading master detected after %v",
		getMesosMasterHostPortTimeout)
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
