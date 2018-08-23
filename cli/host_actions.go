package cli

import (
	"fmt"
	"strings"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	"code.uber.internal/infra/peloton/common/stringset"
)

const (
	// machineID is specified as `hostname:IP`
	hostIPSeparator       = ":"
	hostQueryFormatHeader = "Hostname\tIP\tState\n"
	hostQueryFormatBody   = "%s\t%s\t%s\n"
	machineIDSeparator    = ","
)

// HostMaintenanceStartAction is the action for starting host maintenance. StartMaintenance puts the host(s)
// into DRAINING state by posting a maintenance schedule to Mesos Master. Inverse offers are sent out and
// all future offers from the(se) host(s) are tagged with unavailability (Please check Mesos Maintenance
// Primitives for more info). The hosts are first drained of tasks before they are put into maintenance
// by posting to /machine/down endpoint of Mesos Master.
func (c *Client) HostMaintenanceStartAction(machines string) error {
	machineIDs, err := extractMachineIDs(machines)
	if err != nil {
		return err
	}

	request := &host_svc.StartMaintenanceRequest{
		MachineIds: machineIDs,
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
func (c *Client) HostMaintenanceCompleteAction(machines string) error {
	machineIDs, err := extractMachineIDs(machines)
	if err != nil {
		return err
	}

	request := &host_svc.CompleteMaintenanceRequest{
		MachineIds: machineIDs,
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
// 		1.HostState_HOST_STATE_UP
// 		2.HostState_HOST_STATE_DRAINING
//		3.HostState_HOST_STATE_DRAINED
// 		4.HostState_HOST_STATE_DOWN
func (c *Client) HostQueryAction(states string) error {
	var hostStates []host.HostState
	for _, state := range strings.Split(states, machineIDSeparator) {
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

func extractMachineIDs(machines string) ([]*mesos_v1.MachineID, error) {
	var machineIDs []*mesos_v1.MachineID
	machineSet := stringset.New()
	for _, machine := range strings.Split(machines, machineIDSeparator) {
		// removing leading and trailing white spaces
		machine = strings.TrimSpace(machine)
		if machine == "" {
			return nil, fmt.Errorf("Machine ID cannot be empty")
		}
		if machineSet.Contains(machine) {
			return nil, fmt.Errorf("Invalid input. Duplicate entry for machine %s found", machine)
		}
		machineSet.Add(machine)
		m := strings.Split(machine, hostIPSeparator)
		// Throw error if machineID is malformed
		if len(m) != 2 {
			return nil, fmt.Errorf("Invalid machine ID '%s'. Please specify machineID as <hostname:IP>", machine)
		}
		machineID := &mesos_v1.MachineID{
			Hostname: &m[0],
			Ip:       &m[1],
		}
		machineIDs = append(machineIDs, machineID)
	}
	return machineIDs, nil
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
