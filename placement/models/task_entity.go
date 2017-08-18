package models

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
)

// TaskEntity represents a Peloton task to Mimir placement entity mapping.
type TaskEntity struct {
	Task   *resmgr.Task
	Entity *placement.Entity
}
