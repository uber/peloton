package resmgr

import (
	"sync"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/config"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/task"
	tq "code.uber.internal/infra/peloton/resmgr/taskqueue"
	log "github.com/Sirupsen/logrus"
)

// Server struct for handling the zk election
type Server struct {
	cfg   *config.Config
	mutex *sync.Mutex
	// Local address for peloton Master Resource Manager
	localAddr      string
	respoolService respool.ServiceHandler
	taskQueue      *tq.Queue
	taskSched      *task.Scheduler
}

// NewServer will create the elect handle object
func NewServer(
	cfg *config.Config,
	localResMgrMasterAddr string,
	rm respool.ServiceHandler,
	taskqueue *tq.Queue,
	taskSched *task.Scheduler) *Server {
	result := Server{
		cfg:            cfg,
		mutex:          &sync.Mutex{},
		localAddr:      localResMgrMasterAddr,
		respoolService: rm,
		taskQueue:      taskqueue,
		taskSched:      taskSched,
	}
	return &result
}

// GainedLeadershipCallback is the callback when the current node becomes the leader
func (p *Server) GainedLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.WithFields(log.Fields{"role": common.ResourceManagerRole}).Info("Gained leadership")

	err := p.taskQueue.LoadFromDB()
	if err != nil {
		log.Errorf("Failed to taskQueue.LoadFromDB, err = %v", err)
		return err
	}
	p.respoolService.Start()
	p.taskSched.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (p *Server) LostLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.WithFields(log.Fields{"role": common.ResourceManagerRole}).Info("Lost leadership")
	p.taskQueue.Reset()
	p.respoolService.Stop()
	p.taskSched.Stop()
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
