package resmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/config"
	"code.uber.internal/infra/peloton/resmgr/respool"
	tq "code.uber.internal/infra/peloton/resmgr/taskqueue"
	"code.uber.internal/infra/peloton/yarpc/peer"
	log "github.com/Sirupsen/logrus"
)

// Server struct for handling the zk election
type Server struct {
	peerChooser *peer.Chooser
	cfg         *config.Config
	mutex       *sync.Mutex
	// Local address for peloton Master Resource Manager
	localAddr      string
	env            string
	respoolService respool.ServiceHandler
	taskQueue      *tq.Queue
}

// NewServer will create the elect handle object
func NewServer(env string,
	pChooser *peer.Chooser,
	cfg *config.Config,
	localResMgrMasterAddr string,
	rm respool.ServiceHandler,
	taskqueue *tq.Queue) *Server {
	result := Server{
		env:            env,
		peerChooser:    pChooser,
		cfg:            cfg,
		mutex:          &sync.Mutex{},
		localAddr:      localResMgrMasterAddr,
		respoolService: rm,
		taskQueue:      taskqueue,
	}
	return &result
}

// GainedLeadershipCallback is the callback when the current node becomes the leader
func (p *Server) GainedLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Gained leadership")

	err := p.taskQueue.LoadFromDB()
	if err != nil {
		log.Errorf("Failed to taskQueue.LoadFromDB, err = %v", err)
		return err
	}

	// Gained leadership, Need to start resmgr service
	err = p.peerChooser.UpdatePeer(p.localAddr, common.PelotonResourceManager)
	if err != nil {
		log.Errorf("Failed to update peer with p.localResMgrAddr, err = %v", err)
		return err
	}
	p.respoolService.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (p *Server) LostLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Lost leadership")
	p.taskQueue.Reset()
	p.respoolService.Stop()

	return nil
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (p *Server) ShutDownCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Quiting the election")
	return nil
}

// GetID function returns the peloton resource manager master address
// required to implement leader.Nomination
func (p *Server) GetID() string {
	return p.localAddr
}
