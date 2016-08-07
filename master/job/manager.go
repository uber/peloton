package job

import (
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"
	"code.uber.internal/go-common.git/x/log"
	"peloton/job"
)

func InitManager(d yarpc.Dispatcher) {
	handler := jobManager{}
	json.Register(d, json.Procedure("JobManager.Create", handler.Create))
	json.Register(d, json.Procedure("JobManager.Get", handler.Get))
	json.Register(d, json.Procedure("JobManager.Query", handler.Query))
	json.Register(d, json.Procedure("JobManager.Delete", handler.Delete))
}

type jobManager struct {
}

func (m *jobManager) Create(
	reqMeta yarpc.ReqMeta,
	body *job.CreateRequest) (*job.CreateResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Create called: %s", body)
	return &job.CreateResponse{}, nil, nil
}

func (m *jobManager) Get(
	reqMeta yarpc.ReqMeta,
	body *job.GetRequest) (*job.GetResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Get called: %s", body)
	return &job.GetResponse{}, nil, nil
}

func (m *jobManager) Query(
	reqMeta yarpc.ReqMeta,
	body *job.QueryRequest) (*job.QueryResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Query called: %s", body)
	return &job.QueryResponse{}, nil, nil
}

func (m *jobManager) Delete(
	reqMeta yarpc.ReqMeta,
	body *job.DeleteRequest) (*job.DeleteResponse, yarpc.ResMeta, error) {

	log.Infof("JobManager.Delete called: %s", body)
	return &job.DeleteResponse{}, nil, nil
}

