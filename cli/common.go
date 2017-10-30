package cli

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

// LookupResourcePoolID returns the resource pool ID for a given path
func (c *Client) LookupResourcePoolID(resourcePoolPath string) (*peloton.ResourcePoolID, error) {
	request := &respool.LookupRequest{
		Path: &respool.ResourcePoolPath{
			Value: resourcePoolPath,
		},
	}

	response, err := c.resClient.LookupResourcePoolID(c.ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Id, nil
}
