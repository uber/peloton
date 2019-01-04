package cli

import (
	"fmt"
	"sort"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/uber/peloton/common/stringset"
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

// ExtractHostnames extracts a list of hosts from a comma-separated list
func (c *Client) ExtractHostnames(hosts string, hostSeparator string) ([]string, error) {
	hostSet := stringset.New()
	for _, host := range strings.Split(hosts, hostSeparator) {
		// removing leading and trailing white spaces
		host = strings.TrimSpace(host)
		if host == "" {
			return nil, fmt.Errorf("Host cannot be empty")
		}
		if hostSet.Contains(host) {
			return nil, fmt.Errorf("Invalid input. Duplicate entry for host %s found", host)
		}
		hostSet.Add(host)
	}
	hostSlice := hostSet.ToSlice()
	sort.Strings(hostSlice)
	return hostSlice, nil
}
