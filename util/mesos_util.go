package util

import (
	"fmt"
	"strings"
)

const (
	// ipPortSeparator is the separator for IP address and Port
	ipPortSeparator = ":"
	// slaveIPSeparator is the separator for slave id and IP address
	slaveIPSeparator = "@"
)

// ExtractIPAndPortFromMesosAgentPID parses Mesos PID to extract IP-address
// and port number (if present).
func ExtractIPAndPortFromMesosAgentPID(pid string) (string, string, error) {
	// pid is of the form slave<id>@<ip>:<port>
	pidParts := strings.Split(pid, slaveIPSeparator)
	if len(pidParts) != 2 {
		err := fmt.Errorf("invalid Agent PID: %s", pid)
		return "", "", err
	}
	pidParts[0] = strings.TrimSpace(pidParts[0])
	pidParts[1] = strings.TrimSpace(pidParts[1])
	if pidParts[0] == "" || pidParts[1] == "" {
		err := fmt.Errorf("invalid Agent PID: %s", pid)
		return "", "", err
	}
	pidParts = strings.Split(pidParts[1], ipPortSeparator)
	ip := strings.TrimSpace(pidParts[0])
	if ip == "" {
		err := fmt.Errorf("invalid Agent PID: %s", pid)
		return "", "", err
	}
	var port string
	if len(pidParts) > 1 {
		port = strings.TrimSpace(pidParts[1])
	}
	return ip, port, nil
}
