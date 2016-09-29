// Scheduler Interface
// IN: job
// OUT: placement decision <task, node>
// https://github.com/Netflix/Fenzo

package scheduler

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/util"
	"github.com/pborman/uuid"
	myarpc "code.uber.internal/infra/peloton/yarpc"
	"fmt"
	"mesos/v1"
	sched "mesos/v1/scheduler"
	"peloton/task"
	"sync/atomic"
	"time"
)

const (
	GetOfferTimeout = 10 * time.Second
	GetTaskTimeout  = 1 * time.Second
)

// InitManager inits the schedulerManager
func InitManager(oq util.OfferQueue, tq util.TaskQueue, caller myarpc.Caller) {
	s := schedulerManager{
		offerQueue:  oq,
		taskQueue:   tq,
		mesosCaller: caller,
	}
	s.Start()
}

type schedulerManager struct {
	offerQueue  util.OfferQueue
	taskQueue   util.TaskQueue
	mesosCaller myarpc.Caller
	started     int32
	shutdown    int32
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
						s.LaunchTasks(offer, tasks)
					}
					break
				} else {
					taskId := fmt.Sprintf("%s-%d-%v", task.JobId.Value, task.InstanceId, uuid.NewUUID().String())
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
							s.LaunchTasks(offer, tasks)
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

// LaunchTasks launches the tasks using an offer.
// TODO: consider move this into task manager
func (s *schedulerManager) LaunchTasks(offer *mesos_v1.Offer, pelotonTasks []*task.TaskInfo) {
	callType := sched.Call_ACCEPT
	var offerIds []*mesos_v1.OfferID
	offerIds = append(offerIds, offer.Id)

	var mesosTaskInfos []*mesos_v1.TaskInfo
	var mesosTaskIds []string
	for _, t := range pelotonTasks {
		mesosTask := util.ConvertToMesosTaskInfo(t)
		mesosTask.AgentId = offer.AgentId
		mesosTaskInfos = append(mesosTaskInfos, mesosTask)
		mesosTaskIds = append(mesosTaskIds, *mesosTask.TaskId.Value)
	}
	log.Infof("Launching tasks %v using offer %v", mesosTaskIds, *offer.GetId().Value)
	opType := mesos_v1.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: offer.FrameworkId,
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: offerIds,
			Operations: []*mesos_v1.Offer_Operation{
				&mesos_v1.Offer_Operation{
					Type: &opType,
					Launch: &mesos_v1.Offer_Operation_Launch{
						TaskInfos: mesosTaskInfos,
					},
				},
			},
		},
	}
	// TODO: add retry / put back offer and tasks in failure scenarios
	s.mesosCaller.SendPbRequest(msg)
}
