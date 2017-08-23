package volume

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in volume manager.
type Metrics struct {
	GetVolumeAPI  tally.Counter
	GetVolume     tally.Counter
	GetVolumeFail tally.Counter
}

// NewMetrics returns a new instance of hostmgr.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	subScope := scope.SubScope("volume")
	return &Metrics{
		GetVolume:     subScope.Counter("get_volume"),
		GetVolumeAPI:  subScope.Counter("get_volume_api"),
		GetVolumeFail: subScope.Counter("get_volume_fail"),
	}
}
