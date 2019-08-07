// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"fmt"
	"sort"

	pb_host "github.com/uber/peloton/.gen/peloton/api/v0/host"
	host_svc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
)

const (
	_hostpoolSummaryHeader = "Pool name\tNumber of hosts\n"
	_hostpoolSummaryBody   = "%s\t%8d\n"
)

// HostPoolList lists all the host pools.
func (c *Client) HostPoolList() error {
	resp, err := c.hostClient.ListHostPools(
		c.ctx,
		&host_svc.ListHostPoolsRequest{})

	if err != nil {
		return err
	}

	fmt.Fprintf(tabWriter, _hostpoolSummaryHeader)
	for _, p := range resp.GetPools() {
		fmt.Fprintf(
			tabWriter,
			_hostpoolSummaryBody,
			p.GetName(),
			len(p.GetHosts()))
	}
	tabWriter.Flush()
	return nil
}

// HostPoolListHosts lists all hosts in a host pool.
func (c *Client) HostPoolListHosts(name string) error {
	resp, err := c.hostClient.ListHostPools(
		c.ctx,
		&host_svc.ListHostPoolsRequest{})

	if err != nil {
		return err
	}
	for _, p := range resp.GetPools() {
		if p.GetName() == name {
			hosts := p.GetHosts()
			sort.Strings(hosts)
			for _, h := range hosts {
				fmt.Fprintf(tabWriter, "%s\n", h)
			}
			tabWriter.Flush()
			return nil
		}
	}
	return fmt.Errorf("Pool %q not found", name)
}

// HostPoolCreate creates a host pool.
func (c *Client) HostPoolCreate(name string) error {
	_, err := c.hostClient.CreateHostPool(
		c.ctx,
		&host_svc.CreateHostPoolRequest{Name: name},
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(tabWriter, "Created host pool %q.\n", name)
	tabWriter.Flush()
	return nil
}

// HostPoolDelete deletes a host pool.
func (c *Client) HostPoolDelete(name string) error {
	_, err := c.hostClient.DeleteHostPool(
		c.ctx,
		&host_svc.DeleteHostPoolRequest{Name: name},
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(tabWriter, "Deleted host pool %q.\n", name)
	tabWriter.Flush()
	return nil
}

// HostPoolChangePool changes the pool for a host.
func (c *Client) HostPoolChangePool(host, source, dest string) error {
	if len(source) == 0 {
		srcPool, err := c.getHostPoolForHost(host)
		if err != nil {
			return err
		}
		source = srcPool.GetName()
	}
	_, err := c.hostClient.ChangeHostPool(
		c.ctx,
		&host_svc.ChangeHostPoolRequest{
			Hostname:        host,
			SourcePool:      source,
			DestinationPool: dest,
		},
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(
		tabWriter,
		"Pool for host %q changed to %q.\n",
		host,
		dest)
	tabWriter.Flush()
	return nil
}

func (c *Client) getHostPoolForHost(host string) (
	*pb_host.HostPoolInfo, error,
) {
	resp, err := c.hostClient.ListHostPools(
		c.ctx,
		&host_svc.ListHostPoolsRequest{})

	if err != nil {
		return nil, err
	}
	for _, p := range resp.GetPools() {
		for _, h := range p.GetHosts() {
			if h == host {
				return p, nil
			}
		}
	}
	return nil, fmt.Errorf("Pool for host %q not found", host)
}
