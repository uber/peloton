package task

import (
	"sync"

	hostmgr_mesos "code.uber.internal/infra/peloton/hostmgr/mesos"
	sched_metrics "code.uber.internal/infra/peloton/scheduler/metrics"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
	"peloton/api/task"
)

// Launcher is the interface to launch a set of tasks using an offer
type Launcher interface {
	LaunchTasks(offer *mesos.Offer, tasks []*task.TaskInfo) error
}

type taskLauncher struct {
	client  mpb.Client
	metrics *sched_metrics.Metrics
}

var instance *taskLauncher
var once sync.Once

// GetTaskLauncher returns the task launcher
func GetTaskLauncher(d yarpc.Dispatcher, mesosClient mpb.Client, metrics *sched_metrics.Metrics) Launcher {
	once.Do(func() {
		instance = &taskLauncher{
			client:  mesosClient,
			metrics: metrics,
		}
	})
	return instance
}

// LaunchTasks launches a list of tasks using an offer
func (t *taskLauncher) LaunchTasks(
	offer *mesos.Offer, tasks []*task.TaskInfo) error {
	var mesosTasks []*mesos.TaskInfo
	var mesosTaskIds []string
	for _, t := range tasks {
		mesosTask := util.ConvertToMesosTaskInfo(t)
		mesosTask.AgentId = offer.AgentId
		mesosTasks = append(mesosTasks, mesosTask)
		mesosTaskIds = append(mesosTaskIds, *mesosTask.TaskId.Value)
	}
	callType := sched.Call_ACCEPT
	opType := mesos.Offer_Operation_LAUNCH
	msg := &sched.Call{
		FrameworkId: offer.FrameworkId,
		Type:        &callType,
		Accept: &sched.Call_Accept{
			OfferIds: []*mesos.OfferID{offer.Id},
			Operations: []*mesos.Offer_Operation{
				{
					Type: &opType,
					Launch: &mesos.Offer_Operation_Launch{
						TaskInfos: mesosTasks,
					},
				},
			},
		},
	}
	// TODO: add retry / put back offer and tasks in failure scenarios
	msid := hostmgr_mesos.GetSchedulerDriver().GetMesosStreamID()
	err := t.client.Call(msid, msg)
	if err != nil {
		t.metrics.LaunchTaskFail.Inc(int64(len(mesosTasks)))
		t.metrics.LaunchOfferAcceptFail.Inc(1)
		log.Warnf("Failed to launch %d tasks using offer %v, err=%v",
			len(mesosTasks), *offer.GetId().Value, err)
		return err
	}
	t.metrics.LaunchTask.Inc(int64(len(mesosTasks)))
	t.metrics.LaunchOfferAccept.Inc(1)
	log.Debugf("Launched %d tasks %v using offer %v",
		len(mesosTasks), mesosTaskIds, *offer.GetId().Value)
	return nil
}
