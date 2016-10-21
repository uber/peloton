// Scheduler Interface
// IN: job
// OUT: placement decision <task, node>
// https://github.com/Netflix/Fenzo

package scheduler

import (
	"code.uber.internal/go-common.git/x/log"
	master_task "code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/util"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"golang.org/x/net/context"
	mesos "mesos/v1"
	"peloton/master/offerpool"
	"peloton/master/taskqueue"
	"peloton/task"
	"sync/atomic"
	"time"
)

const (
	GetOfferTimeout      = 1 * time.Second
	GetTaskTimeout       = 1 * time.Second
	defaultTaskBatchSize = 100
)

// InitManager inits the schedulerManager
func InitManager(d yarpc.Dispatcher) {
	s := schedulerManager{
		launcher: master_task.GetTaskLauncher(d),
		client:   json.New(d.Channel("peloton-master")),
		rootCtx:  context.Background(),
	}
	s.Start()
}

type schedulerManager struct {
	dispatcher yarpc.Dispatcher
	client     json.Client
	rootCtx    context.Context
	started    int32
	shutdown   int32
	launcher   master_task.TaskLauncher
}

func (s *schedulerManager) Start() {
	if atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		log.Infof("Scheduler started")
		go s.workLoop()
		return
	}
	log.Warnf("Scheduler already started")
}

func (s *schedulerManager) Stop() {
	log.Infof("scheduler stopping")
	atomic.StoreInt32(&s.shutdown, 1)
}

func (s *schedulerManager) launchTasksLoop(tasks []*task.TaskInfo) {
	nTasks := len(tasks)
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		offers, err := s.getOffers(1)
		if len(offers) == 0 {
			time.Sleep(GetOfferTimeout)
			continue
		}
		if err != nil {
			log.Errorf("Failed to dequeue offer, err=%v", err)
			time.Sleep(GetOfferTimeout)
			continue
		}
		// TODO: handle multiple offer -> multiple tasks assignment
		// for now only get one offer each time
		offer := offers[0]
		tasks = s.useOfferLaunchTasks(tasks, offer)
		if len(tasks) == 0 {
			log.Infof("Tasks all launched, total %v tasks", nTasks)
			return
		}
	}
}

func (s *schedulerManager) useOfferLaunchTasks(tasks []*task.TaskInfo, offer *mesos.Offer) []*task.TaskInfo {
	remain := util.GetOfferScalarResourceSummary(offer)
	offerId := offer.GetId().Value
	log.Infof("Using offer %v to launch tasks", *offerId)
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
	// launch the tasks that can be launched
	if len(selectedTasks) > 0 {
		s.launcher.LaunchTasks(offer, selectedTasks)
	}
	return tasks
}

// workLoop is the internal loop that
func (s *schedulerManager) workLoop() {
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		tasks, err := s.getTasks(defaultTaskBatchSize)
		if len(tasks) == 0 {
			time.Sleep(GetTaskTimeout)
			continue
		}
		if err != nil {
			log.Errorf("Failed to dequeue tasks, err=%v", err)
			time.Sleep(GetTaskTimeout)
			continue
		}
		s.launchTasksLoop(tasks)
		// It could happen that the work loop is started before the peloton master inbound is started.
		// In such case it could panic. This we capture the panic, log, wait and then resume
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic %v", r)
			time.Sleep(GetTaskTimeout)
		}
	}
}

func (s *schedulerManager) getTasks(limit int) ([]*task.TaskInfo, error) {
	ctx, _ := context.WithTimeout(s.rootCtx, 10*time.Second)
	var response taskqueue.DequeueResponse
	var request = &taskqueue.DequeueRequest{
		Limit: uint32(limit),
	}
	_, err := s.client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue"),
		request,
		&response,
	)
	if err != nil {
		log.Errorf("Deque failed with err=%v", err)
		return nil, err
	}
	log.Infof("%d tasks returned", len(response.Tasks))
	return response.Tasks, nil
}

func (s *schedulerManager) getOffers(limit int) ([]*mesos.Offer, error) {
	ctx, _ := context.WithTimeout(s.rootCtx, 10*time.Second)
	var response offerpool.GetOffersResponse
	var request = &offerpool.GetOffersRequest{
		Limit: uint32(limit),
	}
	_, err := s.client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("OfferPool.GetOffers"),
		request,
		&response,
	)
	if err != nil {
		log.Errorf("getOffers failed with err=%V", err)
		return nil, err
	}
	return response.Offers, nil
}
