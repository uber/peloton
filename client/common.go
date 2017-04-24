package client

import (
	"fmt"

	"go.uber.org/yarpc"

	"peloton/api/respool"
)

// LookupResourcePoolID returns the resource pool ID for a given path
func (client *Client) LookupResourcePoolID(resourcePoolPath string) (*respool.ResourcePoolID, error) {
	fmt.Printf("looking up parent id for: %s\n", resourcePoolPath)

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
