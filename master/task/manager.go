package task

import (
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"

	"code.uber.internal/go-common.git/x/log"
	"peloton/task"
)

func InitManager(d yarpc.Dispatcher) {
	handler := taskManager{}
	json.Register(d, json.Procedure("TaskManager.Get", handler.Get))
	json.Register(d, json.Procedure("TaskManager.List", handler.List))
	json.Register(d, json.Procedure("TaskManager.Start", handler.Start))
	json.Register(d, json.Procedure("TaskManager.Stop", handler.Stop))
	json.Register(d, json.Procedure("TaskManager.Restart", handler.Restart))
}

type taskManager struct {
}

func (m *taskManager) Get(
	reqMeta yarpc.ReqMeta,
	body *task.GetRequest) (*task.GetResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Get called: %s", body)
	return &task.GetResponse{}, nil, nil
}

func (m *taskManager) List(
	reqMeta yarpc.ReqMeta,
	body *task.ListRequest) (*task.ListResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.List called: %s", body)
	return &task.ListResponse{}, nil, nil
}

func (m *taskManager) Start(
	reqMeta yarpc.ReqMeta,
	body *task.StartRequest) (*task.StartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Start called: %s", body)
	return &task.StartResponse{}, nil, nil
}

func (m *taskManager) Stop(
	reqMeta yarpc.ReqMeta,
	body *task.StopRequest) (*task.StopResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Stop called: %s", body)
	return &task.StopResponse{}, nil, nil
}

func (m *taskManager) Restart(
	reqMeta yarpc.ReqMeta,
	body *task.RestartRequest) (*task.RestartResponse, yarpc.ResMeta, error) {

	log.Infof("TaskManager.Restart called: %s", body)
	return &task.RestartResponse{}, nil, nil
}
