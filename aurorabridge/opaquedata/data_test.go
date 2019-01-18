package opaquedata

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
)

func TestSerializeAndDeserializeData(t *testing.T) {
	input := &Data{
		UpdateActions: []UpdateAction{
			StartPulsed,
			Pulse,
		},
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

func TestSerializeActions(t *testing.T) {
	od, err := SerializeActions(StartPulsed, Pulse)
	require.NoError(t, err)

	d, err := Deserialize(od)
	require.NoError(t, err)
	require.Equal(t, []UpdateAction{StartPulsed, Pulse}, d.UpdateActions)
}
