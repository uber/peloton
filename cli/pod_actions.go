package cli

import (
	"fmt"

	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	podsvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"
)

// PodGetCache get pod status from cache"
func (c *Client) PodGetCache(podName string) error {
	resp, err := c.podClient.GetPodCache(
		c.ctx,
		&podsvc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
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
