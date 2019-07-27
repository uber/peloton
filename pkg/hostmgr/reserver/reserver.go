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

package reserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/common/queue"
	"github.com/uber/peloton/pkg/hostmgr/config"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/offer/offerpool"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	"github.com/uber/peloton/pkg/hostmgr/util"

	log "github.com/sirupsen/logrus"
)

const (
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noHostsTimeoutPenalty = 1 * time.Second
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noTasksTimeoutPenalty     = 1 * time.Second
	_reserveQueue              = "reserveQueue"
	_completedReservationQueue = "completedReservationQueue"
	// represents the max size of the reserve queue
	_maxReservationQueueSize = 10000
	// max wait time duration for completed reservation queue
	_maxwaitTime = 1 * time.Second
)

var (
	errNoCompletedReservation = errors.New("no completed reservation found")
)

// Reserver represents a host manager's reservation module.
// It gets a list of hosts and resmgr task for which we need
// reservation on the particular host. Reserver will make the
// reservation on the host by which no other offers will be given
// for a particular amount of time. After the timeout, Offers will
// be released and will be given back to placement engines
// after that tasks can be launched on this host.
type Reserver interface {
	// Adding daemon interface for Reserver
	async.Daemon

	// Reserve reserves the task to host in hostmanager
	Reserve(ctx context.Context) (time.Duration, error)

	// FindCompletedReservations finds the completed reservations and
	// puts them into completed reservation queue
	FindCompletedReservations(ctx context.Context) map[string]*hostsvc.Reservation

	// Cancel reservation cancels the reservation on the provided
	// hosts
	CancelReservations(reservations map[string]*hostsvc.Reservation) error

	// EnqueueReservations enqueues the reservation in the reserver queue
	EnqueueReservation(ctx context.Context, reservation *hostsvc.Reservation) error

	// DequeueCompletedReservation dequeues the completed reservations equal to
	// limit from the completed Reservation queue
	DequeueCompletedReservation(ctx context.Context, limit int) ([]*hostsvc.CompletedReservation, error)
}

// reserver is the struct which impelements Reserver interface
type reserver struct {
	lock sync.RWMutex
	// Host Manager config for the reserver
	config *config.Config
	// Host Manager metrics
	metrics *metrics.Metrics
	// daemon object for making reservecr a daemon process
	daemon async.Daemon
	// reserve queue for getting the tasks from placement engine
	// to make the reservation
	reserveQueue queue.Queue
	// completedReservationQueue is the queue which will contain
	// all the fulfilled reservations
	completedReservationQueue queue.Queue
	// map of hostname -> resmgr task for all the reservations been done
	reservations map[string]*hostsvc.Reservation
	offerPool    offerpool.Pool
}

// NewReserver creates a new reserver which gets the list of reservation
// from the reservationQueue and based on the availability of the host it
// chooses the host and put the reservation on the host.
func NewReserver(
	metrics *metrics.Metrics,
	cfg *config.Config,
	offerPool offerpool.Pool) Reserver {
	reserver := &reserver{
		config:       cfg,
		metrics:      metrics,
		offerPool:    offerPool,
		reservations: make(map[string]*hostsvc.Reservation),
	}
	reserver.reserveQueue = queue.NewQueue(
		_reserveQueue,
		reflect.TypeOf(hostsvc.Reservation{}), _maxReservationQueueSize)
	reserver.completedReservationQueue = queue.NewQueue(
		_completedReservationQueue,
		reflect.TypeOf(hostsvc.CompletedReservation{}), _maxReservationQueueSize)
	reserver.daemon = async.NewDaemon("Host Manager Reserver", reserver)
	return reserver
}

// Start method starts the daemon process
func (r *reserver) Start() {
	r.daemon.Start()
}

// Run method implements runnable from daemon
// this is the method which gets called while starting the
// daemon process.
func (r *reserver) Run(ctx context.Context) error {
	timer := time.NewTimer(time.Duration(0))
	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		delay, err := r.Reserve(ctx)
		if err != nil {
			log.WithError(err).Info("tasks can't reserve hosts")
		}
		// Checking if the reservations are completed
		// if yes then pass those reservation to completedReservation Queue
		// by that those reservations can be passed to placement engine
		// for placement
		failedReservations := r.FindCompletedReservations(ctx)
		if len(failedReservations) > 0 {
			r.CancelReservations(failedReservations)
		}
		timer.Reset(delay)
	}
}

// Stop methis will stop the daemon process.
func (r *reserver) Stop() {
	r.daemon.Stop()
}

// EnqueueReservations enqueues the reservation in the reserver queue
func (r *reserver) EnqueueReservation(
	ctx context.Context,
	reservation *hostsvc.Reservation) error {
	return r.reserveQueue.Enqueue(reservation)
}

// DequeueCompletedReservation dequeues the completed reservation
// from the completed Reservation queue
func (r *reserver) DequeueCompletedReservation(ctx context.Context, limit int,
) ([]*hostsvc.CompletedReservation, error) {
	var completedReservations []*hostsvc.CompletedReservation
	for i := 0; i < limit; i++ {
		item, err := r.completedReservationQueue.Dequeue(_maxwaitTime)
		if err != nil {
			break
		}
		completedReservations = append(completedReservations, item.(*hostsvc.CompletedReservation))
	}
	if len(completedReservations) == 0 {
		return nil, errNoCompletedReservation
	}
	return completedReservations, nil
}

// Reserve method is being called from Run method
// This method does following steps
//   1. Get reservations from the reservation queue
//   2. Tries to reserve host means making it
//      RESERVE in host summary
//   3. Once reservation is done keep the mapping in
//      MAP[hostname] -> Reservation object
//   4. After finishing all the reservation, it will also
//      checks what are the reservations are fulfilled.
//   5. Send these fulfilled reservations to reserved queue

func (r *reserver) Reserve(ctx context.Context) (time.Duration, error) {
	// Get Tasks from the reservation queue
	item, err := r.reserveQueue.Dequeue(1 * time.Second)
	if err != nil {
		log.Debug("no items in reservation queue")
		return _noTasksTimeoutPenalty, nil
	}
	reservation, ok := item.(*hostsvc.Reservation)
	if !ok || reservation.GetTask() == nil {
		return _noTasksTimeoutPenalty, fmt.Errorf("not a valid task %s",
			reservation.GetTask())
	}

	hsummary, hInfo := r.addHostReservation(reservation.GetHosts())
	if hsummary == nil && hInfo == nil {
		// we need to add this reservation as failed reservation
		err := r.completedReservationQueue.Enqueue(
			&hostsvc.CompletedReservation{
				Task: reservation.Task,
			})
		if err != nil {
			log.WithError(err).
				WithField("reservation", reservation).
				Errorf("couldn't enqueue failed reservation")
		}
		return time.Duration(_noHostsTimeoutPenalty),
			errors.New("reservation failed")
	}
	// Making the reservation
	r.reservations[hInfo.Hostname] = reservation

	return time.Duration(0), nil
}

// addHostReservation marks the host to RESERVE state and
// update the map of reservation.
func (r *reserver) addHostReservation(reservedHosts []*hostsvc.HostInfo,
) (summary.HostSummary, *hostsvc.HostInfo) {
	// Marking host reserved
	for len(reservedHosts) > 0 {
		hostSummary, hostInfo := r.GetReadyHost(reservedHosts)
		if hostSummary == nil {
			reservedHosts = r.removeFailedReservedHosts(reservedHosts, hostInfo.Hostname)
			continue
		}
		err := hostSummary.CasStatus(hostSummary.GetHostStatus(), summary.ReservedHost)
		if err != nil {
			reservedHosts = r.removeFailedReservedHosts(reservedHosts, hostInfo.Hostname)
			continue
		}
		return hostSummary, hostInfo
	}
	return nil, nil
}

// GetReadyHost checks all the hosts in the list and checks one by one
// the status of each if they are in READY state , it returns that
// for reservation.
func (r *reserver) GetReadyHost(hosts []*hostsvc.HostInfo,
) (summary.HostSummary, *hostsvc.HostInfo) {
	var hostSummary summary.HostSummary
	var hostInfo *hostsvc.HostInfo
	for _, h := range hosts {
		hostInfo = h
		host, err := r.offerPool.GetHostSummary(h.Hostname)
		if err != nil {
			log.WithError(err).Errorf("couldn't find the host %s ", h.Hostname)
			continue
		}
		if host.GetHostStatus() == summary.ReadyHost {
			hostSummary = host
			hostInfo = h
			break
		}
	}
	return hostSummary, hostInfo
}

// removeFailedReservedHosts removes the host from the list
// and returns the rest of the hosts.
func (r *reserver) removeFailedReservedHosts(hosts []*hostsvc.HostInfo,
	hostName string) []*hostsvc.HostInfo {
	remainingHosts := make([]*hostsvc.HostInfo, 0, len(hosts)-1)
	for _, h := range hosts {
		if h.Hostname == hostName {
			continue
		}
		remainingHosts = append(remainingHosts, h)
	}
	return remainingHosts
}

// FindCompletedReservations looks into current reservations and
// try to find out if the hosts are having outstanding offers
// for reservations to fulfil the reservation. If yes it will put
// them into completed reservation queue and remove from the reservation list
func (r *reserver) FindCompletedReservations(ctx context.Context,
) map[string]*hostsvc.Reservation {
	r.lock.Lock()
	defer r.lock.Unlock()
	failedReservations := make(map[string]*hostsvc.Reservation)
	for host, res := range r.reservations {
		hostResources := r.getResourcesFromHostOffers(host)
		taskResources := scalar.FromResourceConfig(res.GetTask().GetResource())
		if hostResources.Contains(taskResources) {
			err := r.returnReservation(res, host)
			if err != nil {
				log.WithFields(
					log.Fields{
						"reservation": res,
						"host":        host}).
					WithError(err).
					Debug("fail to reserve host")

				failedReservations[host] = res
				continue
			}
		}
	}
	return failedReservations
}

// returnReservation moves the host status from Reserve to Placing
// gets the host offers and make it part of the reservation
func (r *reserver) returnReservation(res *hostsvc.Reservation, host string) error {
	s, err := r.offerPool.GetHostSummary(host)
	if err != nil {
		return err
	}
	err = s.CasStatus(summary.ReservedHost, summary.PlacingHost)

	if err != nil {
		return err
	}
	offers := s.GetOffers(summary.Unreserved)
	mesosOffers := make([]*mesos_v1.Offer, 0, len(offers))
	mesosOfferMap := make(map[string][]*mesos_v1.Offer)
	for _, o := range offers {
		mesosOffers = append(mesosOffers, o)
	}
	mesosOfferMap[host] = mesosOffers
	err = r.completeHostReservation(
		r.createCompletedReservation(
			res,
			host,
			util.MesosOffersToHostOffer(s.GetHostOfferID(), mesosOffers)),
		host)
	if err != nil {
		return err
	}

	log.WithField("reservation", res).
		WithField("host", host).
		Debug("host reservation completed")
	return nil
}

func (r *reserver) createCompletedReservation(
	reservation *hostsvc.Reservation,
	host string,
	offer *hostsvc.HostOffer) *hostsvc.CompletedReservation {
	var hInfo *hostsvc.HostInfo
	for _, hostInfo := range reservation.GetHosts() {
		if hostInfo.Hostname == host {
			hInfo = hostInfo
		}
	}
	return &hostsvc.CompletedReservation{
		Task:      reservation.GetTask(),
		Host:      hInfo,
		HostOffer: offer,
	}
}

// CancelReservations takes the map of reservations and cancel those
// hosts reservation and remove from the list as well as put those reservation
// to the completed queue
func (r *reserver) CancelReservations(
	reservations map[string]*hostsvc.Reservation) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	var err error
	for host, res := range reservations {
		err = r.makeHostAvailable(host)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"reservation": res,
				"host":        host,
			}).Error("reservation can not be cancelled")
		}
		err = r.completeHostReservation(
			r.createCompletedReservation(
				res,
				host,
				nil), // making offers nil as this reservation is cancelled
			host)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"reservation": res,
				"host":        host,
			}).Error("reservation can not be cancelled")
		}
	}
	return err
}

// makeHostAvailable changes the host status from reserved to Ready
func (r *reserver) makeHostAvailable(host string) error {
	s, err := r.offerPool.GetHostSummary(host)
	if err != nil {
		return err
	}
	err = s.CasStatus(summary.ReservedHost, summary.ReadyHost)
	log.WithFields(log.Fields{
		"old_host_status": s.GetHostStatus(),
		"new_host_staus":  summary.ReadyHost,
	}).Info("Changing host status to be READY")

	if err != nil {
		return err
	}
	return nil
}

// completeHostReservation puts reservation to completed queue
func (r *reserver) completeHostReservation(res *hostsvc.CompletedReservation,
	host string) error {
	if res == nil {
		return errors.New("invalid completed Reservation")
	}
	err := r.completedReservationQueue.Enqueue(res)
	if err != nil {
		return err
	}
	delete(r.reservations, host)
	return nil
}

// getResourcesFromHostOffers gets all the outstanding offers from host and returns
// the total resources.
func (r *reserver) getResourcesFromHostOffers(hostname string) scalar.Resources {
	var total scalar.Resources
	sum, err := r.offerPool.GetHostSummary(hostname)
	if err != nil {
		log.WithError(err).Errorf("couldn't find the host %s ", hostname)
		return total
	}
	offers := sum.GetOffers(summary.Unreserved)
	for _, offer := range offers {
		total = total.Add(scalar.FromMesosResources(offer.GetResources()))
	}
	return total
}
