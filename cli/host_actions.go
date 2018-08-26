package cli

import (
	"fmt"
	"strings"

	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	"code.uber.internal/infra/peloton/common/stringset"
)

const (
	hostQueryFormatHeader = "Hostname\tIP\tState\n"
	hostQueryFormatBody   = "%s\t%s\t%s\n"
	hostSeparator         = ","
)

// HostMaintenanceStartAction is the action for starting host maintenance. StartMaintenance puts the host(s)
// into DRAINING state by posting a maintenance schedule to Mesos Master. Inverse offers are sent out and
// all future offers from the(se) host(s) are tagged with unavailability (Please check Mesos Maintenance
// Primitives for more info). The hosts are first drained of tasks before they are put into maintenance
// by posting to /machine/down endpoint of Mesos Master.
// The hosts transition from UP to DRAINING and finally to DOWN.
func (c *Client) HostMaintenanceStartAction(hosts string) error {
	hostnames, err := extractHostnames(hosts)
	if err != nil {
		return err
	}

	request := &host_svc.StartMaintenanceRequest{
		Hostnames: hostnames,
	}
	_, err = c.hostClient.StartMaintenance(c.ctx, request)
	if err != nil {
		return err
	}

	fmt.Fprintf(tabWriter, "Started draining hosts\n")
	tabWriter.Flush()
	return nil
}

// HostMaintenanceCompleteAction is the action for completing host maintenance. Complete maintenance brings UP a host
// which is in maintenance by posting to /machine/up endpoint of Mesos Master i.e. the machine transitions from DOWN to
// UP state (Please check Mesos Maintenance Primitives for more info)
func (c *Client) HostMaintenanceCompleteAction(hosts string) error {
	hostnames, err := extractHostnames(hosts)
	if err != nil {
		return err
	}

	request := &host_svc.CompleteMaintenanceRequest{
		Hostnames: hostnames,
	}
	_, err = c.hostClient.CompleteMaintenance(c.ctx, request)
	if err != nil {
		return err
	}

	fmt.Fprintf(tabWriter, "Maintenance completed\n")
	tabWriter.Flush()
	return nil
}

// HostQueryAction is the action for querying hosts by states. This can be to used to monitor the state of the host(s)
// Eg. When a list of hosts are put into maintenance (`host maintenance start`).
// A host, at any given time, will be in one of the following states
// 		1.HostState_HOST_STATE_UP - The host is up and running
// 		2.HostState_HOST_STATE_DRAINING - The tasks running on the host are being rescheduled and
// 										  there will be no further placement of tasks on the host
//		3.HostState_HOST_STATE_DRAINED - There are no tasks running on this host and it is ready to be 'DOWN'ed
// 		4.HostState_HOST_STATE_DOWN - The host is in maintenance.
func (c *Client) HostQueryAction(states string) error {
	var hostStates []host.HostState
	for _, state := range strings.Split(states, hostSeparator) {
		if state != "" {
			hostStates = append(hostStates, host.HostState(host.HostState_value[state]))
		}
	}

	// If states is empty then query for all states
	if len(hostStates) == 0 {
		for state := range host.HostState_name {
			hostStates = append(hostStates, host.HostState(state))
		}
	}

	request := &host_svc.QueryHostsRequest{
		HostStates: hostStates,
	}
	response, err := c.hostClient.QueryHosts(c.ctx, request)
	if err != nil {
		return err
	}

	printHostQueryResponse(response, c.Debug)
	return nil
}

func extractHostnames(hosts string) ([]string, error) {
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
	return hostSet.ToSlice(), nil
}

func printHostQueryResponse(r *host_svc.QueryHostsResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if len(r.GetHostInfos()) == 0 {
			fmt.Fprintf(tabWriter, "No hosts found\n")
			return
		}
		fmt.Fprintf(tabWriter, hostQueryFormatHeader)
		for _, h := range r.GetHostInfos() {
			fmt.Fprintf(
				tabWriter,
				hostQueryFormatBody,
				h.GetHostname(),
				h.GetIp(),
				h.GetState(),
			)
		}
	}
	tabWriter.Flush()
}
