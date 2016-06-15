package task

import (
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"

	"code.uber.internal/go-common.git/x/log"
	"peloton/task"
)

func InitManager(rpc yarpc.RPC) {
	handler := taskManager{}
	json.Register(rpc, json.Procedure("get", handler.Get))
	json.Register(rpc, json.Procedure("list", handler.List))
	json.Register(rpc, json.Procedure("start", handler.Start))
	json.Register(rpc, json.Procedure("stop", handler.Stop))
	json.Register(rpc, json.Procedure("restart", handler.Restart))
}

type taskManager struct {
}

func (m *taskManager) Get(
	reqMeta *json.ReqMeta,
	body *task.GetRequest) (*task.GetResponse, *json.ResMeta, error) {

	log.Infof("TaskManager.get called: %s", body)
	return &task.GetResponse{}, nil, nil
}

func (m *taskManager) List(
	reqMeta *json.ReqMeta,
	body *task.ListRequest) (*task.ListResponse, *json.ResMeta, error) {

	log.Infof("TaskManager.list called: %s", body)
	return &task.ListResponse{}, nil, nil
}

func (m *taskManager) Start(
	reqMeta *json.ReqMeta,
	body *task.StartRequest) (*task.StartResponse, *json.ResMeta, error) {

	log.Infof("TaskManager.start called: %s", body)
	return &task.StartResponse{}, nil, nil
}

func (m *taskManager) Stop(
	reqMeta *json.ReqMeta,
	body *task.StopRequest) (*task.StopResponse, *json.ResMeta, error) {

	log.Infof("TaskManager.stop called: %s", body)
	return &task.StopResponse{}, nil, nil
}

func (m *taskManager) Restart(
	reqMeta *json.ReqMeta,
	body *task.RestartRequest) (*task.RestartResponse, *json.ResMeta, error) {

	log.Infof("TaskManager.restart called: %s", body)
	return &task.RestartResponse{}, nil, nil
}
