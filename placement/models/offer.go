package models

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
)

// Offer represents a Peloton offer and the tasks running on it.
type Offer struct {
	Offer *hostsvc.HostOffer
	Tasks []*resmgr.Task
}
