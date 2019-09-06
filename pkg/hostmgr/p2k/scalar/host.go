package scalar

import (
	"strconv"

	"github.com/uber/peloton/pkg/hostmgr/models"
	hmscalar "github.com/uber/peloton/pkg/hostmgr/scalar"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
)

// HostEventType describes the type of host event sent by plugin.
type HostEventType int

const (
	// AddHost event type.
	AddHost HostEventType = iota + 1
	// UpdateHostSpec event type.
	UpdateHostSpec
	// DeleteHost event type.
	DeleteHost
	// UpdateHostAvailableRes event type, used by mesos only
	UpdateHostAvailableRes
	//  UpdateAgent event type, used by mesos only
	UpdateAgent
)

// HostEvent contains information about the host, event type and resource
// version for that event.
type HostEvent struct {
	// Host info struct for that host.
	hostInfo *HostInfo
	// Type of host event.
	eventType HostEventType
}

// GetEventType is helper function to get event type.
func (h *HostEvent) GetEventType() HostEventType {
	return h.eventType
}

// GetHostInfo is helper function to get host info.
func (h *HostEvent) GetHostInfo() *HostInfo {
	return h.hostInfo
}

// HostInfo contains the host specific information we receive from underlying
// scheduler (k8s or mesos).
type HostInfo struct {
	// Host name for this host.
	hostname string
	// Map of podID to allocated resources for pods running on this host.
	podMap map[string]models.HostResources
	// Actual capacity of this host.
	capacity models.HostResources
	// Available resources on the host.
	available models.HostResources
	// Resource version for this host. This is k8s specific.
	resourceVersion string
}

// GetHostName is helper function to get name of the host.
func (h *HostInfo) GetHostName() string {
	return h.hostname
}

// GetCapacity is helper function to get capacity for the host.
func (h *HostInfo) GetCapacity() models.HostResources {
	return h.capacity
}

// GetAvailable is helper function to get available resources for the host.
func (h *HostInfo) GetAvailable() models.HostResources {
	return h.available
}

// GetPodMap is helper function to get pod map for the host.
func (h *HostInfo) GetPodMap() map[string]models.HostResources {
	return h.podMap
}

// GetResourceVersion is helper function to get resource version.
func (h *HostInfo) GetResourceVersion() string {
	return h.resourceVersion
}

// Initialize each host disk capacity to 1T by default for k8s.
// This is because k8s does not have concept of disk resource.
func getDefaultDiskMbPerHost() float64 {
	r := resource.MustParse("1Ti")
	return float64(r.MilliValue() / 1000000000)
}

// BuildHostEventFromNode builds a host event from underlying k8s node object.
func BuildHostEventFromNode(
	node *corev1.Node,
	e HostEventType,
) (*HostEvent, error) {
	// TODO: create podMap (map of podID to resource).
	podMap := make(map[string]models.HostResources)
	rv, err := meta.NewAccessor().ResourceVersion(node)
	if err != nil {
		return nil, err
	}

	nonSlackCap := hmscalar.Resources{
		CPU: float64(
			node.Status.Capacity.Cpu().MilliValue()) / 1000,
		Mem: float64(
			node.Status.Capacity.Memory().MilliValue()) / 1000000000,
		Disk: getDefaultDiskMbPerHost(),
		GPU:  0,
	}

	return &HostEvent{
		hostInfo: &HostInfo{
			hostname: node.Name,
			podMap:   podMap,
			capacity: models.HostResources{
				Slack:    hmscalar.Resources{},
				NonSlack: nonSlackCap,
			},
			resourceVersion: rv,
		},
		eventType: e,
	}, nil
}

// BuildHostEventFromResource builds a host event from underlying resource
func BuildHostEventFromResource(
	hostname string,
	available models.HostResources,
	capacity models.HostResources,
	e HostEventType,
) *HostEvent {
	podMap := make(map[string]models.HostResources)
	return &HostEvent{
		hostInfo: &HostInfo{
			hostname:  hostname,
			podMap:    podMap,
			available: available,
			capacity:  capacity,
		},
		eventType: e,
	}
}

// IsOldVersion is a very k8s specific check.
// TODO: make this an interface with a noop impl for Mesos.
// Check if the event has already been received. When we start k8s node
// and pod informers, we start getting events with a reference version. On the
// first sync up, all nodes in the system will send an "add" event to peloton
// On a subsequent list, (list being a time consuming operation), we may get
// older events. By caching the resource version in memory, we should be able
// to check for and reject older events. Kubernetes internally uses this same
// check to identify older events. As per their developer guidelines, it should
// be safe to do it here. Further reference:
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
func IsOldVersion(oldVersion, newVersion string) bool {
	oldV, _ := strconv.ParseUint(oldVersion, 10, 64)
	newV, _ := strconv.ParseUint(newVersion, 10, 64)
	return newV < oldV
}
