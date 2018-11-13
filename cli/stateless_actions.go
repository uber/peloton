package cli

import (
	"fmt"

	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
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
