package cli

import (
	"go.uber.org/yarpc"

	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

// LookupResourcePoolID returns the resource pool ID for a given path
func (client *Client) LookupResourcePoolID(resourcePoolPath string) (*respool.ResourcePoolID, error) {
	request := &respool.LookupRequest{
		Path: &respool.ResourcePoolPath{
			Value: resourcePoolPath,
		},
	}

	var response respool.LookupResponse
	_, err := client.resClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("ResourceManager.LookupResourcePoolID"),
		request,
		&response,
	)
	if err != nil {
		return nil, err
	}
	return response.Id, nil
}
