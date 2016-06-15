package job

import (
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"code.uber.internal/go-common.git/x/log"
	"peloton/job"
)

func InitManager(rpc yarpc.RPC) {
	handler := jobManager{}
	json.Register(rpc, json.Procedure("create", handler.Create))
	json.Register(rpc, json.Procedure("get", handler.Get))
	json.Register(rpc, json.Procedure("query", handler.Query))
	json.Register(rpc, json.Procedure("delete", handler.Delete))
}

type jobManager struct {
}

func (m *jobManager) Create(
	reqMeta *json.ReqMeta,
	body *job.CreateRequest) (*job.CreateResponse, *json.ResMeta, error) {

	log.Infof("JobManager.create called: %s", body)
	return &job.CreateResponse{}, nil, nil
}

func (m *jobManager) Get(
	reqMeta *json.ReqMeta,
	body *job.GetRequest) (*job.GetResponse, *json.ResMeta, error) {

	log.Infof("JobManager.get called: %s", body)
	return &job.GetResponse{}, nil, nil
}

func (m *jobManager) Query(
	reqMeta *json.ReqMeta,
	body *job.QueryRequest) (*job.QueryResponse, *json.ResMeta, error) {

	log.Infof("JobManager.query called: %s", body)
	return &job.QueryResponse{}, nil, nil
}

func (m *jobManager) Delete(
	reqMeta *json.ReqMeta,
	body *job.DeleteRequest) (*job.DeleteResponse, *json.ResMeta, error) {

	log.Infof("JobManager.delete called: %s", body)
	return &job.DeleteResponse{}, nil, nil
}

