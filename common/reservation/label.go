package reservation

import (
	"fmt"
	"strconv"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"

	"code.uber.internal/infra/peloton/util"
)

var (
	_jobKey      = "job"
	_instanceKey = "instance"
	_hostnameKey = "hostname"
)

// CreateReservationLabels creates reservation labels for stateful task.
func CreateReservationLabels(
	jobID string, instanceID int, hostname string) *mesos.Labels {
	return &mesos.Labels{
		Labels: []*mesos.Label{
			{
				Key:   &_jobKey,
				Value: &jobID,
			},
			{
				Key:   &_instanceKey,
				Value: util.PtrPrintf(strconv.Itoa(instanceID)),
			},
			{
				Key:   &_hostnameKey,
				Value: util.PtrPrintf(hostname),
			},
		},
	}
}

// ParseReservationLabels parses jobid and instanceid from given reservation labels.
func ParseReservationLabels(labels *mesos.Labels) (string, uint32, error) {
	var jobID string
	instanceID := -1
	// iterates all the label and parse out jobID and instanceID.
	for _, label := range labels.GetLabels() {
		if label.GetKey() == _jobKey {
			jobID = label.GetValue()
		} else if label.GetKey() == _instanceKey {
			instanceID, _ = strconv.Atoi(label.GetValue())
		}
	}
	// jobID and instanceID are required in reservation labels.
	if len(jobID) == 0 || instanceID == -1 {
		return jobID, uint32(instanceID), fmt.Errorf("invalid reservation labels: %v", labels)
	}
	return jobID, uint32(instanceID), nil
}
