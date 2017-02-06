package election

import (
	"sync"

	"code.uber.internal/infra/peloton/resmgr"
	"code.uber.internal/infra/peloton/resmgr/config"
	tq "code.uber.internal/infra/peloton/resmgr/taskqueue"
	"code.uber.internal/infra/peloton/yarpc/peer"
	log "github.com/Sirupsen/logrus"
)

// Handler struct for handling the zk election
type Handler struct {
	peerChooser *peer.Chooser
	cfg         *config.Config
	mutex       *sync.Mutex
	// Local address for peloton Master Resource Manager
	localAddr       string
	env             string
	resourceManager *resmgr.ResourceManager
	taskQueue       *tq.Queue
}

// NewElectionHandler will create the elect handle object
func NewElectionHandler(env string,
	pChooser *peer.Chooser,
	cfg *config.Config,
	localResMgrMasterAddr string,
	rm *resmgr.ResourceManager,
	taskqueue *tq.Queue) *Handler {
	result := Handler{
		env:             env,
		peerChooser:     pChooser,
		cfg:             cfg,
		mutex:           &sync.Mutex{},
		localAddr:       localResMgrMasterAddr,
		resourceManager: rm,
		taskQueue:       taskqueue,
	}
	return &result
}

// GainedLeadershipCallBack is the callback when the current node becomes the leader
func (p *Handler) GainedLeadershipCallBack() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Gained leadership")

	err := p.taskQueue.LoadFromDB()
	if err != nil {
		log.Errorf("Failed to taskQueue.LoadFromDB, err = %v", err)
		return err
	}

	// Gained leadership, Need to start resmgr service
	err = p.peerChooser.UpdatePeer(p.localAddr)
	if err != nil {
		log.Errorf("Failed to update peer with p.localResMgrAddr, err = %v", err)
		return err
	}
	p.resourceManager.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (p *Handler) LostLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Lost leadership")
	p.taskQueue.Reset()
	p.resourceManager.Stop()

	return nil
}

// NewLeaderCallBack is the callback when some other node becomes the leader, leader is hostname of the leader
func (p *Handler) NewLeaderCallBack(leader string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	log.Infof("New Leader is elected : %v", leader)
	// leader changes, so point resmgrOutbound to the new leader
	return p.peerChooser.UpdatePeer(leader)
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (p *Handler) ShutDownCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Quiting the election")
	return nil
}

// GetHostPort function returns the peloton resource manager master address
func (p *Handler) GetHostPort() string {
	return p.localAddr
}
