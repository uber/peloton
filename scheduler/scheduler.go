// Scheduler Interface
// IN: job
// OUT: placement decision <task, node>
// https://github.com/Netflix/Fenzo

package scheduler

import (
	"sync/atomic"
	"time"

	"github.com/yarpc/yarpc-go"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/util"
	master_task "code.uber.internal/infra/peloton/master/task"

	"peloton/task"
)

const (
	GetOfferTimeout = 10 * time.Second
	GetTaskTimeout  = 1 * time.Second
)

// InitManager inits the schedulerManager
func InitManager(d yarpc.Dispatcher, oq util.OfferQueue, tq util.TaskQueue) {
	s := schedulerManager{
		offerQueue: oq,
		taskQueue:  tq,
		launcher:   master_task.GetTaskLauncher(d),
	}
	s.Start()
}

type schedulerManager struct {
	offerQueue util.OfferQueue
	taskQueue  util.TaskQueue
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

// workLoop is the internal loop that
func (s *schedulerManager) workLoop() {
	for shutdown := atomic.LoadInt32(&s.shutdown); shutdown == 0; {
		offer := s.offerQueue.GetOffer(GetOfferTimeout)
		if offer != nil {
			offerId := offer.GetId().Value
			log.Infof("Get an offer from the queue, offer : %v", *offerId)
			var ok = false
			var remain = util.GetOfferScalarResourceSummary(offer)
			var tasks []*task.TaskInfo
			for {
				task := s.taskQueue.GetTask(GetTaskTimeout)
				if task == nil {
					log.Infof("Task queue empty, admitted tasks to launch: %v, offerId %v", len(tasks), *offerId)
					// No task to launch, put the offer back to queue
					if len(tasks) == 0 {
						continue
					} else {
						err := s.launcher.LaunchTasks(offer, tasks)
						if err != nil {
							log.Errorf("Failed to launch tasks with offer %v: %v", *offerId, err)
							// TODO: handle task launch errors
						}
					}
					break
				} else {
					taskId := task.GetRuntime().GetTaskId().GetValue()
					log.Infof("Task queue dequeue task %v, offerId %v", taskId, *offerId)
					ok = util.CanTakeTask(&remain, task)
					if ok {
						log.Infof("Admiting task %v, with the remaining offer value %v", taskId, remain)
						tasks = append(tasks, task)
					} else {
						// the offer cannot take the task. Put the task back and launch the other tasks
						log.Infof("Put the task %v back to queue", taskId)
						s.taskQueue.PutTask(task)
						if len(tasks) == 0 {
							log.Infof("No task can be launched, put the offer %v back to queue", offer)
							s.offerQueue.PutOffer(offer)
						} else {
							s.launcher.LaunchTasks(offer, tasks)
						}
						break
					}
				}
			}
		} else {
			log.Infof("No offer from offerQueue")
		}
	}
}
