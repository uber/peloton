// Scheduler Interface
// IN: job
// OUT: placement decision <task, node>
// https://github.com/Netflix/Fenzo

package scheduler

import (
	"code.uber.internal/go-common.git/x/log"
	master_task "code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"fmt"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"golang.org/x/net/context"
	mesos "mesos/v1"
	"peloton/master/offerpool"
	"peloton/master/taskqueue"
	"peloton/task"
	"sync/atomic"
	"time"
)

const (
	GetOfferTimeout = 1 * time.Second
	GetTaskTimeout  = 1 * time.Second
)

// InitManager inits the schedulerManager
func InitManager(d yarpc.Dispatcher, cfg *Config, mesosClient mpb.Client) {
	s := schedulerManager{
		cfg:      cfg,
		launcher: master_task.GetTaskLauncher(d, mesosClient),
		client:   json.New(d.Channel("peloton-master")),
		rootCtx:  context.Background(),
	}
	s.Start()
}

type schedulerManager struct {
	dispatcher yarpc.Dispatcher
	cfg        *Config
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
	log.Infof("Scheduler stopping")
	atomic.StoreInt32(&s.shutdown, 1)
}

func (s *schedulerManager) launchTasksLoop(tasks []*task.TaskInfo) {
	nTasks := len(tasks)
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		offers, err := s.getOffers(1)
		if err != nil {
			log.Errorf("Failed to dequeue offer, err=%v", err)
			time.Sleep(GetOfferTimeout)
			continue
		}
		if len(offers) == 0 {
			time.Sleep(GetOfferTimeout)
			continue
		}
		// TODO: handle multiple offer -> multiple tasks assignment
		// for now only get one offer each time
		offer := offers[0]
		tasks = s.assignTasksToOffer(tasks, offer)
		if len(tasks) == 0 {
			break
		}
	}
	log.Debugf("Launched all %v tasks", nTasks)
}

func (s *schedulerManager) assignTasksToOffer(
	tasks []*task.TaskInfo, offer *mesos.Offer) []*task.TaskInfo {
	remain := util.GetOfferScalarResourceSummary(offer)
	offerId := offer.GetId().Value
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
		// TODO: handle task launch error and reschedule the tasks
		s.launcher.LaunchTasks(offer, selectedTasks)

		log.Infof("Launched %v tasks on %v using offer %v", len(selectedTasks),
			offer.GetHostname(), *offerId)
	}
	return tasks
}

// workLoop is the internal loop that
func (s *schedulerManager) workLoop() {
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
		s.launchTasksLoop(tasks)
	}
}

func (s *schedulerManager) getTasks(limit int) (
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

	ctx, _ := context.WithTimeout(s.rootCtx, 10*time.Second)
	var response taskqueue.DequeueResponse
	var request = &taskqueue.DequeueRequest{
		Limit: uint32(limit),
	}
	_, err = s.client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("TaskQueue.Dequeue"),
		request,
		&response,
	)
	if err != nil {
		log.Errorf("Deque failed with err=%v", err)
		return nil, err
	}
	return response.Tasks, nil
}

func (s *schedulerManager) getOffers(limit int) (
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

	ctx, _ := context.WithTimeout(s.rootCtx, 10*time.Second)
	var response offerpool.GetOffersResponse
	var request = &offerpool.GetOffersRequest{
		Limit: uint32(limit),
	}
	_, err = s.client.Call(
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
