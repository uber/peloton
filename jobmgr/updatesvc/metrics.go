package updatesvc

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the update service
type Metrics struct {
	UpdateAPICreate    tally.Counter
	UpdateCreate       tally.Counter
	UpdateCreateFail   tally.Counter
	UpdateAPIGet       tally.Counter
	UpdateGet          tally.Counter
	UpdateGetFail      tally.Counter
	UpdateAPIList      tally.Counter
	UpdateList         tally.Counter
	UpdateListFail     tally.Counter
	UpdateAPIGetCache  tally.Counter
	UpdateGetCache     tally.Counter
	UpdateGetCacheFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	UpdateSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	UpdateFailScope := scope.Tagged(map[string]string{"result": "fail"})
	UpdateAPIScope := scope.SubScope("api")

	return &Metrics{
		UpdateAPICreate:    UpdateAPIScope.Counter("create"),
		UpdateCreate:       UpdateSuccessScope.Counter("create"),
		UpdateCreateFail:   UpdateFailScope.Counter("create"),
		UpdateAPIGet:       UpdateAPIScope.Counter("get"),
		UpdateGet:          UpdateSuccessScope.Counter("get"),
		UpdateGetFail:      UpdateFailScope.Counter("get"),
		UpdateAPIList:      UpdateAPIScope.Counter("list"),
		UpdateList:         UpdateSuccessScope.Counter("list"),
		UpdateListFail:     UpdateFailScope.Counter("list"),
		UpdateAPIGetCache:  UpdateAPIScope.Counter("cache_get"),
		UpdateGetCache:     UpdateSuccessScope.Counter("cache_get"),
		UpdateGetCacheFail: UpdateFailScope.Counter("cache_get"),
	}
}
