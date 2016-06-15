/**
 * Upgrade Manager Handler
 * 1. Diff the new job config with existing one
 * 2. Create workflow actions ??
 * 3. Call orchestrator to run the workflow ??
 *    - Embeded orchestrator (one per peloton cluster)
 *    - YARPC support (HTTP/JSON)
 */

package upgrade

import (
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/encoding/json"

	"code.uber.internal/go-common.git/x/log"
	"peloton/upgrade"
)

func InitManager(rpc yarpc.RPC) {
	handler := upgradeManager{}
	json.Register(rpc, json.Procedure("create", handler.Create))
	json.Register(rpc, json.Procedure("get", handler.Get))
	json.Register(rpc, json.Procedure("list", handler.List))
	json.Register(rpc, json.Procedure("pause", handler.Pause))
	json.Register(rpc, json.Procedure("resume", handler.Resume))
	json.Register(rpc, json.Procedure("rollback", handler.Rollback))
	json.Register(rpc, json.Procedure("abort", handler.Abort))
}

type upgradeManager struct {
}

func (m *upgradeManager) Create(
	reqMeta *json.ReqMeta,
	body *upgrade.CreateRequest) (*upgrade.CreateResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.create called: %s", body)
	return &upgrade.CreateResponse{}, nil, nil
}

func (m *upgradeManager) Get(
	reqMeta *json.ReqMeta,
body *upgrade.GetRequest) (*upgrade.GetResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.get called: %s", body)
	return &upgrade.GetResponse{}, nil, nil
}

func (m *upgradeManager) List(
	reqMeta *json.ReqMeta,
	body *upgrade.ListRequest) (*upgrade.ListResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.list called: %s", body)
	return &upgrade.ListResponse{}, nil, nil
}

func (m *upgradeManager) Pause(
	reqMeta *json.ReqMeta,
	body *upgrade.PauseRequest) (*upgrade.PauseResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.pause called: %s", body)
	return &upgrade.PauseResponse{}, nil, nil
}

func (m *upgradeManager) Resume(
	reqMeta *json.ReqMeta,
	body *upgrade.ResumeRequest) (*upgrade.ResumeResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.resume called: %s", body)
	return &upgrade.ResumeResponse{}, nil, nil
}

func (m *upgradeManager) Rollback(
	reqMeta *json.ReqMeta,
	body *upgrade.RollbackRequest) (*upgrade.RollbackResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.rollback called: %s", body)
	return &upgrade.RollbackResponse{}, nil, nil
}

func (m *upgradeManager) Abort(
	reqMeta *json.ReqMeta,
	body *upgrade.AbortRequest) (*upgrade.AbortResponse, *json.ResMeta, error) {

	log.Infof("UpgradeManager.abprt called: %s", body)
	return &upgrade.AbortResponse{}, nil, nil
}
