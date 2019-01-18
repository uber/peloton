package opaquedata

import (
	"encoding/json"
	"fmt"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
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
	UpdateActions []UpdateAction `json:"update_actions"`
}

// AppendUpdateAction appends a to d's update actions.
func (d *Data) AppendUpdateAction(a UpdateAction) {
	d.UpdateActions = append(d.UpdateActions, a)
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

// SerializeActions is a convenience function for directly serializing a list
// of update actions.
func SerializeActions(actions ...UpdateAction) (*peloton.OpaqueData, error) {
	d := &Data{}
	for _, a := range actions {
		d.AppendUpdateAction(a)
	}
	return d.Serialize()
}
