package opaquedata

import (
	"encoding/json"
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/pborman/uuid"
)

// UpdateAction is an Aurora update action which has no equivalent in Peloton.
type UpdateAction string

const (
	// StartPulsed represents a startJobUpdate request with a non-zero
	// blockIfNoPulsesAfterMs.
	StartPulsed UpdateAction = "start_pulsed"

	// Pulse represents a pulseJobUpdate request.
	Pulse = "pulse"

	// Rollback represents a rollbackJobUpdate request.
	Rollback = "rollback"
)

// Data is used to annotate Peloton updates with information that does not
// directly map into the Peloton API. It has no meaning in Peloton, however can
// be used when converting Peloton objects into Aurora objects to reconstruct
// Aurora states which do not exist in Peloton.
type Data struct {
	UpdateID              string          `json:"update_id,omitempty"`
	UpdateActions         []UpdateAction  `json:"update_actions,omitempty"`
	UpdateMetadata        []*api.Metadata `json:"update_metadata,omitempty"`
	StartJobUpdateMessage string          `json:"start_job_update_msg,omitempty"`
}

// NewDataFromJobUpdateRequest creates opaquedata.Data from aurora
// JobUpdateRequest
func NewDataFromJobUpdateRequest(
	request *api.JobUpdateRequest,
	message *string,
) *Data {
	d := &Data{
		UpdateID:       uuid.New(),
		UpdateMetadata: request.GetMetadata(),
	}
	if request.GetSettings().GetBlockIfNoPulsesAfterMs() > 0 {
		d.AppendUpdateAction(StartPulsed)
	}
	if message != nil {
		d.StartJobUpdateMessage = *message
	}
	return d
}

// AppendUpdateAction appends a to d's update actions.
func (d *Data) AppendUpdateAction(a UpdateAction) {
	d.UpdateActions = append(d.UpdateActions, a)
}

// ContainsUpdateAction returns true if d contains a.
func (d *Data) ContainsUpdateAction(a UpdateAction) bool {
	for _, da := range d.UpdateActions {
		if da == a {
			return true
		}
	}
	return false
}

// IsLatestUpdateAction returns true if a is the latest UpdateAction in d.
func (d *Data) IsLatestUpdateAction(a UpdateAction) bool {
	return len(d.UpdateActions) > 0 && d.UpdateActions[len(d.UpdateActions)-1] == a
}

// Deserialize deserializes od into a Data struct. If od is empty, an empty Data
// struct is returned.
func Deserialize(od *peloton.OpaqueData) (*Data, error) {
	d := &Data{}
	if od.GetData() != "" {
		if err := json.Unmarshal([]byte(od.GetData()), d); err != nil {
			return nil, fmt.Errorf("json unmarshal: %s", err)
		}
	}
	return d, nil
}

// Serialize converts d into OpaqueData. Produces stable results.
func (d *Data) Serialize() (*peloton.OpaqueData, error) {
	b, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %s", err)
	}
	return &peloton.OpaqueData{Data: string(b)}, nil
}
