package cli

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"

	"github.com/stretchr/testify/assert"
)

func TestMarshallResponse(t *testing.T) {
	var respose = &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Config: &job.JobConfig{
				Name:          "test job",
				Description:   "test job",
				InstanceCount: 1,
			},
		},
	}

	ret, err := marshallResponse(jsonResponseFormat, respose)
	assert.Nil(t, err)
	expected := "{\n  \"jobInfo\": {\n    \"config\": {\n" +
		"      \"description\": \"test job\",\n" +
		"      \"instanceCount\": 1,\n      \"name\": \"test job\",\n" +
		"      \"owningTeam\": \"\",\n      \"type\": \"BATCH\"\n    },\n" +
		"    \"id\": {\n      \"value\": \"481d565e-28da-457d-8434-f6bb7faa0e95\"\n    }\n  }\n}"
	assert.Equal(t, expected, string(ret))

	ret, err = marshallResponse(defaultResponseFormat, respose)
	assert.Nil(t, err)
	expected = "jobInfo:\n  config:\n    description: test job\n" +
		"    instanceCount: 1\n    name: test job\n    owningTeam: \"\"\n    type: BATCH\n" +
		"  id:\n    value: 481d565e-28da-457d-8434-f6bb7faa0e95\n"
	assert.Equal(t, expected, string(ret))
}
