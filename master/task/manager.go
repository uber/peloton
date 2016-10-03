package task

import (
	"fmt"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"golang.org/x/net/context"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
	myarpc "code.uber.internal/infra/peloton/yarpc"
	"code.uber.internal/infra/peloton/yarpc/encoding/mjson"
	"mesos/v1"
	sched "mesos/v1/scheduler"
	"peloton/job"
	"peloton/task"
)

func InitManager(d yarpc.Dispatcher, jobStore storage.JobStore, taskStore storage.TaskStore, oq util.OfferQueue, tq util.TaskQueue, mc myarpc.Caller) util.TaskLauncher {
	handler := taskManager{
		TaskStore:   taskStore,
		JobStore:    jobStore,
		mesosCaller: mc,
		offerQueue:  oq,
		taskQueue:   tq,
	}
	json.Register(d, json.Procedure("TaskManager.Get", handler.Get))
	json.Register(d, json.Procedure("TaskManager.List", handler.List))
	json.Register(d, json.Procedure("TaskManager.Start", handler.Start))
	json.Register(d, json.Procedure("TaskManager.Stop", handler.Stop))
	json.Register(d, json.Procedure("TaskManager.Restart", handler.Restart))

	procedures := map[sched.Event_Type]interface{}{
		sched.Event_UPDATE: handler.Update,
	}
	for typ, hdl := range procedures {
		name := typ.String()
		mjson.Register(d, mesos.ServiceName, mjson.Procedure(name, hdl))
	}

	return &handler
}

type taskManager struct {
	TaskStore   storage.TaskStore
	JobStore    storage.JobStore
	mesosCaller myarpc.Caller
	offerQueue  util.OfferQueue
	taskQueue   util.TaskQueue
}

func (m *taskManager) Get(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.GetRequest) (*task.GetResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Get called: %v", body)
	jobConfig, err := m.JobStore.GetJob(body.JobId)
	if err != nil || jobConfig == nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.GetResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("job %v not found, %v", body.JobId, err),
			},
		}, nil, nil
	}

	result, err := m.TaskStore.GetTaskForJob(body.JobId, body.InstanceId)
	for _, taskInfo := range result {
		log.Infof("found task %v", taskInfo)
		return &task.GetResponse{
			Result: taskInfo,
		}, nil, nil
	}

	return &task.GetResponse{
		OutOfRange: &task.InstanceIdOutOfRange{
			JobId:         body.JobId,
			InstanceCount: jobConfig.InstanceCount,
		},
	}, nil, nil
}

func (m *taskManager) List(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.ListRequest) (*task.ListResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.List called: %v", body)
	_, err := m.JobStore.GetJob(body.JobId)
	if err != nil {
		log.Errorf("Failed to find job with id %v, err=%v", body.JobId, err)
		return &task.ListResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("Failed to find job with id %v, err=%v", body.JobId, err),
			},
		}, nil, nil
	}
	result, err := m.TaskStore.GetTasksForJobByRange(body.JobId, body.Range)
	if err != nil || len(result) == 0 {
		return &task.ListResponse{
			NotFound: &job.JobNotFound{
				Id:      body.JobId,
				Message: fmt.Sprintf("err= %v", err),
			},
		}, nil, nil
	}
	return &task.ListResponse{
		Result: &task.ListResponse_Result{
			Value: result,
		},
	}, nil, nil
}

func (m *taskManager) Start(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StartRequest) (*task.StartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Start called: %v", body)
	return &task.StartResponse{}, nil, nil
}

func (m *taskManager) Stop(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.StopRequest) (*task.StopResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Stop called: %v", body)
	return &task.StopResponse{}, nil, nil
}

func (m *taskManager) Restart(
	ctx context.Context,
	reqMeta yarpc.ReqMeta,
	body *task.RestartRequest) (*task.RestartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Restart called: %v", body)
	return &task.RestartResponse{}, nil, nil
}

// LaunchTasks launches a list of tasks using an offer
func (m *taskManager) LaunchTasks(offer *mesos_v1.Offer, pelotonTasks []*task.TaskInfo) {
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
	m.mesosCaller.SendPbRequest(msg)
}

// Update is the Mesos callback on mesos state updates
func (m *taskManager) Update(
	reqMeta yarpc.ReqMeta, body *sched.Event) error {
	taskUpdate := body.GetUpdate()
	log.WithField("Task update", taskUpdate).Infof("taskManager: Update called")

	taskId := taskUpdate.GetStatus().GetTaskId().GetValue()
	taskInfo, err := m.TaskStore.GetTaskById(taskId)
	if err != nil {
		log.Errorf("Fail to find taskInfo for taskId %v, err=%v", taskId, err)
		return err
	}
	state := util.MesosStateToPelotonState(taskUpdate.GetStatus().GetState())
	taskInfo.GetRuntime().State = state
	err = m.TaskStore.UpdateTask(taskInfo)
	if err != nil {
		log.Errorf("Fail to update taskInfo for taskId %v, new state %v, err=%v", taskId, state, err)
		return err
	}
	return nil
}
