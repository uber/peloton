// Placement Engine Interface
// IN: job
// OUT: placement decision <task, node>
// https://github.com/Netflix/Fenzo

package placement

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	mesos "mesos/v1"
	"peloton/api/task"
	"peloton/private/hostmgr/hostsvc"
	"peloton/private/hostmgr/offerpool"
	"peloton/private/resmgr/taskqueue"

	placement_config "code.uber.internal/infra/peloton/placement/config"
	placement_metrics "code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/util"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
)

const (
	// GetOfferTimeout is the timeout value for get offer request
	GetOfferTimeout = 1 * time.Second
	// GetTaskTimeout is the timeout value for get task request
	GetTaskTimeout = 1 * time.Second
)

// Engine is an interface implementing a way to Start and Stop the placement engine
type Engine interface {
	Start()
	Stop()
}

// New creates a new placement engine
func New(
	d yarpc.Dispatcher,
	cfg *placement_config.PlacementConfig,
	scope tally.Scope,
	resMgrClientName string,
	hostMgrClientName string) Engine {
	metrics := placement_metrics.New(scope)
	s := placementEngine{
		cfg:           cfg,
		resMgrClient:  json.New(d.ClientConfig(resMgrClientName)),
		hostMgrClient: json.New(d.ClientConfig(hostMgrClientName)),
		rootCtx:       context.Background(),
		metrics:       &metrics,
		offerQueue:    util.NewMemLocalOfferQueue("localOfferQueue"),
	}
	return &s
}

type placementEngine struct {
	dispatcher    yarpc.Dispatcher
	cfg           *placement_config.PlacementConfig
	resMgrClient  json.Client
	hostMgrClient json.Client
	rootCtx       context.Context
	started       int32
	shutdown      int32
	metrics       *placement_metrics.Metrics
	offerQueue    util.OfferQueue
}

// Start starts placement engine
func (s *placementEngine) Start() {
	if atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		log.Infof("Placement Engine started")
		s.metrics.Running.Update(1)
		go s.workLoop()
		return
	}
	log.Warnf("Placement Engine already started")
}

// Stop stops placement engine
func (s *placementEngine) Stop() {
	log.Infof("Placement Engine stopping")
	s.metrics.Running.Update(0)
	atomic.StoreInt32(&s.shutdown, 1)
}

// placeTasksLoop is the internal loop that makes placement decisions on tasks
func (s *placementEngine) placeTasksLoop(tasks []*task.TaskInfo) {
	nTasks := len(tasks)
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		// TODO: switch to hostmgr.GetHostOffers api
		offer, err := s.getLocalOffer()
		if err != nil {
			log.Errorf("Failed to dequeue offer, err=%v", err)
			s.metrics.OfferGetFail.Inc(1)
			time.Sleep(GetOfferTimeout)
			continue
		}
		if offer == nil {
			s.metrics.OfferStarved.Inc(1)
			time.Sleep(GetOfferTimeout)
			continue
		}
		s.metrics.OfferGet.Inc(1)
		// TODO: handle multiple offer -> multiple tasks assignment
		// for now only get one offer each time
		tasks = s.placeTasks(tasks, offer)
		if len(tasks) == 0 {
			break
		}
	}
	log.Debugf("Launched all %v tasks", nTasks)
}

// placeTasks makes placement decisions by assigning tasks to offer
func (s *placementEngine) placeTasks(
	tasks []*task.TaskInfo, offer *mesos.Offer) []*task.TaskInfo {
	remain := util.GetOfferScalarResourceSummary(offer)
	offerID := offer.GetId().Value
	nTasks := len(tasks)
	var selectedTasks []*task.TaskInfo
	for i := 0; i < nTasks; i++ {
		ok := util.CanTakeTask(&remain, tasks[len(tasks)-1])
		if ok {
			selectedTasks = append(selectedTasks, tasks[len(tasks)-1])
			tasks = tasks[:len(tasks)-1]
		} else {
			break
		}
	}
	// TODO: replace launch task with resmgr.SetPlacement once it's implemented,
	//       and move task launching logic into Jobmgr
	if len(selectedTasks) > 0 {
		ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
		defer cancelFunc()
		var response hostsvc.LaunchTasksResponse
		var request = util.GetLaunchTasksRequest(selectedTasks, offer)
		_, err := s.hostMgrClient.Call(
			ctx,
			yarpc.NewReqMeta().Procedure("InternalHostService.LaunchTasks"),
			request,
			&response,
		)
		// TODO: add retry / put back offer and tasks in failure scenarios
		if err != nil {
			log.Errorf("Failed to launch %d tasks, err=%v", len(selectedTasks), err)
			s.metrics.TaskLaunchDispatchesFail.Inc(1)
			return tasks
		}
		if response.Error != nil {
			log.Errorf("Failed to launch %d tasks, response.Error=%v", len(selectedTasks), response.Error)
			s.metrics.TaskLaunchDispatchesFail.Inc(1)
			return tasks
		}
		s.metrics.TaskLaunchDispatches.Inc(1)

		log.Infof("Launched %v tasks on %v using offer %v", len(selectedTasks),
			offer.GetHostname(), *offerID)
	}
	return tasks
}

// workLoop is the internal loop that gets tasks and makes placement decisions
func (s *placementEngine) workLoop() {
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		tasks, err := s.getTasks(s.cfg.TaskDequeueLimit)
		if err != nil {
			log.Errorf("Failed to dequeue tasks, err=%v", err)
			time.Sleep(GetTaskTimeout)
			continue
		}
		if len(tasks) == 0 {
			time.Sleep(GetTaskTimeout)
			continue
		}
		log.Infof("Dequeued %v tasks from task queue", len(tasks))
		s.placeTasksLoop(tasks)
	}
}

// getTasks deques tasks from task queue in resource manager
func (s *placementEngine) getTasks(limit int) (
	taskInfos []*task.TaskInfo, err error) {
	// It could happen that the work loop is started before the
	// peloton master inbound is started.  In such case it could
	// panic. This we capture the panic, return error, wait then
	// resume
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Recovered from panic %v", r)
		}
	}()

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response taskqueue.DequeueResponse
	var request = &taskqueue.DequeueRequest{
		Limit: uint32(limit),
	}
	_, err = s.resMgrClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue"),
		request,
		&response,
	)
	if err != nil {
		log.Errorf("Dequeue failed with err=%v", err)
		return nil, err
	}
	return response.Tasks, nil
}

func (s *placementEngine) getLocalOffer() (*mesos.Offer, error) {
	for {
		offer := s.offerQueue.GetOffer(1 * time.Millisecond)
		if offer != nil {
			return offer, nil
		}
		offers, err := s.getOffers(s.cfg.OfferDequeueLimit)
		if err != nil {
			return nil, err
		}
		log.Infof("Get %v offers from offerPool", len(offers))
		if offers != nil && len(offers) > 0 {
			for _, o := range offers {
				s.offerQueue.PutOffer(o)
			}
		} else {
			return nil, nil
		}
	}
}

func (s *placementEngine) getOffers(limit int) (
	offers []*mesos.Offer, err error) {
	// It could happen that the work loop is started before the
	// peloton master inbound is started.  In such case it could
	// panic. This we capture the panic, return error, wait then
	// resume
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Recovered from panic %v", r)
		}
	}()

	ctx, cancelFunc := context.WithTimeout(s.rootCtx, 10*time.Second)
	defer cancelFunc()
	var response offerpool.GetOffersResponse
	var request = &offerpool.GetOffersRequest{
		Limit: uint32(limit),
	}
	_, err = s.hostMgrClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("OfferPool.GetOffers"),
		request,
		&response,
	)
	if err != nil {
		log.Errorf("getOffers failed with err=%v", err)
		return nil, err
	}
	return response.Offers, nil
}
