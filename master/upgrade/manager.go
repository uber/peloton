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

func InitManager(d yarpc.Dispatcher) {
	handler := upgradeManager{}
	json.Register(d, json.Procedure("UpgradeManager.Create", handler.Create))
	json.Register(d, json.Procedure("UpgradeManager.Get", handler.Get))
	json.Register(d, json.Procedure("UpgradeManager.List", handler.List))
	json.Register(d, json.Procedure("UpgradeManager.Pause", handler.Pause))
	json.Register(d, json.Procedure("UpgradeManager.Resume", handler.Resume))
	json.Register(d, json.Procedure("UpgradeManager.Rollback", handler.Rollback))
	json.Register(d, json.Procedure("UpgradeManager.Abort", handler.Abort))
}

type upgradeManager struct {
}

func (m *upgradeManager) Create(
	reqMeta yarpc.ReqMeta,
	body *upgrade.CreateRequest) (*upgrade.CreateResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.Create called: %s", body)
	return &upgrade.CreateResponse{}, nil, nil
}

func (m *upgradeManager) Get(
	reqMeta yarpc.ReqMeta,
	body *upgrade.GetRequest) (*upgrade.GetResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.Get called: %s", body)
	return &upgrade.GetResponse{}, nil, nil
}

func (m *upgradeManager) List(
	reqMeta yarpc.ReqMeta,
	body *upgrade.ListRequest) (*upgrade.ListResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.List called: %s", body)
	return &upgrade.ListResponse{}, nil, nil
}

func (m *upgradeManager) Pause(
	reqMeta yarpc.ReqMeta,
	body *upgrade.PauseRequest) (*upgrade.PauseResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.Pause called: %s", body)
	return &upgrade.PauseResponse{}, nil, nil
}

func (m *upgradeManager) Resume(
	reqMeta yarpc.ReqMeta,
	body *upgrade.ResumeRequest) (*upgrade.ResumeResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.Resume called: %s", body)
	return &upgrade.ResumeResponse{}, nil, nil
}

func (m *upgradeManager) Rollback(
	reqMeta yarpc.ReqMeta,
	body *upgrade.RollbackRequest) (*upgrade.RollbackResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.Rollback called: %s", body)
	return &upgrade.RollbackResponse{}, nil, nil
}

func (m *upgradeManager) Abort(
	reqMeta yarpc.ReqMeta,
	body *upgrade.AbortRequest) (*upgrade.AbortResponse, yarpc.ResMeta, error) {

	log.Infof("UpgradeManager.Abort called: %s", body)
	return &upgrade.AbortResponse{}, nil, nil
}
