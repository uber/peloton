package cli

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"fmt"
	"io/ioutil"

	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"gopkg.in/yaml.v2"
)

// StatelessGetCacheAction get cache of stateless job
func (c *Client) StatelessGetCacheAction(jobID string) error {
	resp, err := c.statelessClient.GetJobCache(
		c.ctx,
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: jobID},
		})
	if err != nil {
		return err
	}

	out, err := marshallResponse(defaultResponseFormat, resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))

	tabWriter.Flush()
	return nil
}

// StatelessRefreshAction refreshes a job
func (c *Client) StatelessRefreshAction(jobID string) error {
	resp, err := c.statelessClient.RefreshJob(
		c.ctx,
		&statelesssvc.RefreshJobRequest{
			JobId: &v1alphapeloton.JobID{Value: jobID},
		})
	if err != nil {
		return err
	}

	out, err := marshallResponse(defaultResponseFormat, resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))

	tabWriter.Flush()
	return nil
}

// StatelessReplaceJobAction updates job by replace its config
func (c *Client) StatelessReplaceJobAction(
	jobID string,
	spec string,
	batchSize uint32,
	respoolPath string,
	entityVersion string,
	override bool,
	maxInstanceRetries uint32,
	maxTolerableInstanceFailures uint32,
	rollbackOnFailure bool,
	startPaused bool,
) error {
	// TODO: implement cli override check and get entity version
	// form job after stateless.Get is ready
	var jobSpec stateless.JobSpec

	// read the job configuration
	buffer, err := ioutil.ReadFile(spec)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", spec, err)
	}
	if err := yaml.Unmarshal(buffer, &jobSpec); err != nil {
		return fmt.Errorf("unable to parse file %s: %v", spec, err)
	}

	// fetch the resource pool id
	respoolID, err := c.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return fmt.Errorf("unable to find resource pool ID for "+
			":%s", respoolPath)
	}

	// set the resource pool id
	jobSpec.RespoolId = &v1alphapeloton.ResourcePoolID{Value: respoolID.GetValue()}

	req := &statelesssvc.ReplaceJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: jobID},
		Version: &v1alphapeloton.EntityVersion{Value: entityVersion},
		Spec:    &jobSpec,
		UpdateSpec: &stateless.UpdateSpec{
			BatchSize:                    batchSize,
			RollbackOnFailure:            rollbackOnFailure,
			MaxInstanceRetries:           maxInstanceRetries,
			MaxTolerableInstanceFailures: maxTolerableInstanceFailures,
			StartPaused:                  startPaused,
		},
	}

	resp, err := c.statelessClient.ReplaceJob(c.ctx, req)
	if err != nil {
		return err
	}

	fmt.Printf("New EntityVersion: %s\n", resp.GetVersion().GetValue())

	return nil
}
