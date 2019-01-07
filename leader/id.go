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

package leader

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"

	log "github.com/sirupsen/logrus"
)

// ID defines the json struct to be encoded in leader node
// TODO(rcharles) add Instance representing the instance number
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
		log.WithError(err).Fatal("failed to get ip")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.WithError(err).Fatal("failed to get hostname")
	}

	// TODO: Populate the version in ID
	id := &ID{
		Hostname: hostname,
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

// ServiceInstance represents znode for Peloton Bridge
// to mock Aurora leader
type ServiceInstance struct {
	ServiceEndpoint     Endpoint            `json:"serviceEndpoint"`
	AdditionalEndpoints map[string]Endpoint `json:"additionalEndpoints"`
	Status              string              `json:"status"`
}

// NewServiceInstance creates a new instance for Aurora
func NewServiceInstance(endpoint Endpoint, additionalEndpoints map[string]Endpoint) string {
	serviceInstance := &ServiceInstance{
		ServiceEndpoint:     endpoint,
		AdditionalEndpoints: additionalEndpoints,
		Status:              "ALIVE",
	}

	serviceInstanceJSON, _ := json.Marshal(serviceInstance)
	return string(serviceInstanceJSON)
}

// Endpoint represents on instance for aurora leader/follower
type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// NewEndpoint creates a new endpoint
func NewEndpoint(port int) Endpoint {
	hostname, err := os.Hostname()
	if err != nil {
		return Endpoint{}
	}
	endpoint := Endpoint{
		Host: hostname,
		Port: port,
	}

	return endpoint
}
