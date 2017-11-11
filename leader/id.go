package leader

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"

	log "github.com/sirupsen/logrus"
)

// ID defines the json struct to be encoded in leader node
type ID struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
	HTTPPort int    `json:"http"`
	GRPCPort int    `json:"grpc"`
	Version  string `json:"version"`
}

// NewID returns a ID for a server to implement leader.Nomination
func NewID(httpPort int, grpcPort int) string {
	ip, err := listenIP()
	if err != nil {
		log.WithError(err).Fatal("Failed to get ip")
	}
	// TODO: Populate the hostname and version in ID
	id := &ID{
		Hostname: "",
		IP:       fmt.Sprintf("%s", ip),
		HTTPPort: httpPort,
		GRPCPort: grpcPort,
		Version:  "",
	}
	idString, _ := json.Marshal(id)
	return string(idString)
}

// scoreAddr scores how likely the given addr is to be a remote
// address and returns the IP to use when listening. Any address which
// receives a negative score should not be used.  Scores are
// calculated as:

// -1 for any unknown IP addreseses.
// +300 for IPv4 addresses
// +100 for non-local addresses, extra +100 for "up" interaces.
// Copied from https://github.com/uber/tchannel-go/blob/dev/localip.go

func scoreAddr(iface net.Interface, addr net.Addr) (int, net.IP) {
	var ip net.IP
	if netAddr, ok := addr.(*net.IPNet); ok {
		ip = netAddr.IP
	} else if netIP, ok := addr.(*net.IPAddr); ok {
		ip = netIP.IP
	} else {
		return -1, nil
	}

	var score int
	if ip.To4() != nil {
		score += 300
	}
	if iface.Flags&net.FlagLoopback == 0 && !ip.IsLoopback() {
		score += 100
		if iface.Flags&net.FlagUp != 0 {
			score += 100
		}
	}
	return score, ip
}

// listenIP returns the IP to bind to in Listen. It tries to find an
// IP that can be used by other machines to reach this machine.
// Copied from https://github.com/uber/tchannel-go/blob/dev/localip.go

func listenIP() (net.IP, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	bestScore := -1
	var bestIP net.IP
	// Select the highest scoring IP as the best IP.
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			// Skip this interface if there is an error.
			continue
		}

		for _, addr := range addrs {
			score, ip := scoreAddr(iface, addr)
			if score > bestScore {
				bestScore = score
				bestIP = ip
			}
		}
	}

	if bestScore == -1 {
		return nil, errors.New("no addresses to listen on")
	}

	return bestIP, nil
}
