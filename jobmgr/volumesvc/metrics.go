package volumesvc

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in volume manager.
type Metrics struct {
	GetVolumeAPI  tally.Counter
	GetVolume     tally.Counter
	GetVolumeFail tally.Counter

	ListVolumeAPI  tally.Counter
	ListVolume     tally.Counter
	ListVolumeFail tally.Counter

	DeleteVolumeAPI  tally.Counter
	DeleteVolume     tally.Counter
	DeleteVolumeFail tally.Counter
}

// NewMetrics returns a new instance of volumesvc.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	subScope := scope.SubScope("volume")
	return &Metrics{
		GetVolume:     subScope.Counter("get"),
		GetVolumeAPI:  subScope.Counter("get_api"),
		GetVolumeFail: subScope.Counter("get_fail"),

		ListVolume:     subScope.Counter("list"),
		ListVolumeAPI:  subScope.Counter("list_api"),
		ListVolumeFail: subScope.Counter("list_fail"),

		DeleteVolume:     subScope.Counter("delete"),
		DeleteVolumeAPI:  subScope.Counter("delete_api"),
		DeleteVolumeFail: subScope.Counter("delete_fail"),
	}
}
