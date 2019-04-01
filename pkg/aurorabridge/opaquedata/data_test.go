package opaquedata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/thriftrw/ptr"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
)

func TestSerializeAndDeserializeData(t *testing.T) {
	input := &Data{
		UpdateID: "some-update-id",
		UpdateActions: []UpdateAction{
			StartPulsed,
			Pulse,
		},
		UpdateMetadata: []*api.Metadata{
			{Key: ptr.String("some key"), Value: ptr.String("some value")},
			{Key: ptr.String("another key"), Value: ptr.String("another value")},
		},
		StartJobUpdateMessage: "some-start-job-update-msg",
	}
	od, err := input.Serialize()
	require.NoError(t, err)

	output, err := Deserialize(od)
	require.NoError(t, err)
	require.Equal(t, input, output)
}

func TestDeserializeEmptyOpaqueData(t *testing.T) {
	od := &peloton.OpaqueData{Data: ""}
	d, err := Deserialize(od)
	require.NoError(t, err)
	require.Equal(t, &Data{}, d)
}

func TestIsLatestUpdateAction(t *testing.T) {
	testCases := []struct {
		name    string
		actions []UpdateAction
		input   UpdateAction
		want    bool
	}{
		{
			"latest of multiple actions",
			[]UpdateAction{StartPulsed, Pulse},
			Pulse,
			true,
		}, {
			"not latest of multiple actions",
			[]UpdateAction{StartPulsed, Pulse},
			StartPulsed,
			false,
		}, {
			"latest of single action",
			[]UpdateAction{StartPulsed},
			StartPulsed,
			true,
		}, {
			"not latest of single action",
			[]UpdateAction{StartPulsed},
			Pulse,
			false,
		}, {
			"empty actions",
			nil,
			Pulse,
			false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := &Data{UpdateActions: tc.actions}
			assert.Equal(t, tc.want, d.IsLatestUpdateAction(tc.input))
		})
	}
}

func TestContainsUpdateAction(t *testing.T) {
	testCases := []struct {
		name    string
		actions []UpdateAction
		input   UpdateAction
		want    bool
	}{
		{
			"contains",
			[]UpdateAction{StartPulsed, Pulse},
			Pulse,
			true,
		}, {
			"not contains",
			[]UpdateAction{StartPulsed},
			Pulse,
			false,
		}, {
			"empty",
			nil,
			Pulse,
			false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			d := &Data{UpdateActions: tc.actions}
			assert.Equal(t, tc.want, d.ContainsUpdateAction(tc.input))
		})
	}
}
