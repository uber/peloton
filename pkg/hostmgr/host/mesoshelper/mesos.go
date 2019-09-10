package mesoshelper

import (
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_maintenance "github.com/uber/peloton/.gen/mesos/v1/maintenance"

	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
)

// RegisterHostAsDown registers a host as down
func RegisterHostAsDown(
	mesosMasterClient mpb.MasterOperatorClient,
	hostname, IP string,
) error {
	resp, err := mesosMasterClient.GetMaintenanceStatus()
	if err != nil {
		return err
	}

	// If host is already down, noop
	// required for idempotency
	downMachines := resp.GetStatus().GetDownMachines()
	if IsHostPartOfDownMachines(hostname, downMachines) {
		log.WithField("hostname", hostname).Info("host already registered as down")
		return nil
	}

	// Start maintenance on the host by posting to
	// /machine/down endpoint of the Mesos Master

	machineID := &mesos.MachineID{
		Hostname: &hostname,
		Ip:       &IP,
	}

	if err := mesosMasterClient.StartMaintenance([]*mesos.MachineID{machineID}); err != nil {
		log.WithError(err).
			WithField("machine", machineID).
			Error("failed to down host")
		return err
	}

	log.WithField("hostname", hostname).Info("host successfully registered as down")

	return nil
}

// RegisterHostAsUp registers a host as up
func RegisterHostAsUp(
	mesosMasterClient mpb.MasterOperatorClient,
	hostname, IP string,
) error {
	// Verify if host already up
	// required for idempotency
	if host.IsHostRegistered(hostname) {
		log.WithField("hostname", hostname).Info("host already registered as up")
		return nil
	}

	// Only complete maintenance and reactivate the host
	// if the host is not already back up on Mesos Master

	// Stop Maintenance for the host on Mesos Master
	machineID := &mesos.MachineID{
		Hostname: &hostname,
		Ip:       &IP,
	}

	if err := mesosMasterClient.StopMaintenance([]*mesos.MachineID{machineID}); err != nil {
		return err
	}

	log.WithField("hostname", hostname).Info("host successfully registered as up")

	return nil
}

// AddHostToMaintenanceSchedule adds a host to the
// Mesos Master maintenance schedule
func AddHostToMaintenanceSchedule(
	mesosMasterClient mpb.MasterOperatorClient,
	hostname, IP string,
) error {
	// Get current maintenance schedule
	response, err := mesosMasterClient.GetMaintenanceSchedule()
	if err != nil {
		return err
	}
	schedule := response.GetSchedule()

	// Verify if host already part of Mesos Master maintenance schedule
	// required for idempotency
	if IsHostAlreadyOnMaintenanceSchedule(hostname, schedule) {
		log.WithField("hostname", hostname).
			Info("host already part of maintenance schedule on Mesos Master")
		return nil
	}

	// Set current time as the `start` of maintenance window
	nanos := time.Now().UnixNano()

	// The maintenance duration has no real significance. A machine can be put into
	// maintenance even after its maintenance window has passed. According to Mesos,
	// omitting the duration means that the unavailability will last forever. Since
	// we do not know the duration, we are omitting it.

	machineID := &mesos.MachineID{
		Hostname: &hostname,
		Ip:       &IP,
	}

	// Construct updated maintenance window including new host
	maintenanceWindow := &mesos_maintenance.Window{
		MachineIds: []*mesos.MachineID{machineID},
		Unavailability: &mesos.Unavailability{
			Start: &mesos.TimeInfo{
				Nanoseconds: &nanos,
			},
		},
	}
	schedule.Windows = append(schedule.Windows, maintenanceWindow)

	// Post updated maintenance schedule
	if err = mesosMasterClient.UpdateMaintenanceSchedule(schedule); err != nil {
		return err
	}

	log.WithField("maintenance_schedule", schedule).
		Debug("Maintenance Schedule posted to Mesos Master")

	log.WithField("hostname", hostname).
		Info("host successfully added to Mesos Master maintenance schedule")

	return nil
}

// IsHostAlreadyOnMaintenanceSchedule returns whether a hostname is
// already part of the Mesos Master maintenance schedule
func IsHostAlreadyOnMaintenanceSchedule(
	hostname string,
	schedule *mesos_maintenance.Schedule,
) bool {
	for _, window := range schedule.Windows {
		for _, machineID := range window.GetMachineIds() {
			if hostname == machineID.GetHostname() {
				return true
			}
		}
	}
	return false
}

// IsHostPartOfDownMachines returns whether a host is registered
// as a down machine on Mesos Master
func IsHostPartOfDownMachines(
	hostname string,
	downMachines []*mesos.MachineID,
) bool {
	for _, downMachine := range downMachines {
		if hostname == downMachine.GetHostname() {
			return true
		}
	}
	return false
}
