package reserver

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/async"
	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/placement/config"
	"code.uber.internal/infra/peloton/placement/hosts"
	tally_metrics "code.uber.internal/infra/peloton/placement/metrics"
	"code.uber.internal/infra/peloton/placement/models"

	log "github.com/sirupsen/logrus"
)

const (
	// number of randomized hosts which will be choosen from
	// all the hosts
	_randomizedHosts = 10
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noHostsTimeoutPenalty = 1 * time.Second
	// _noTasksTimeoutPenalty is the timeout value for a get tasks request.
	_noTasksTimeoutPenalty     = 1 * time.Second
	_completedReservationQueue = "completed-ReservationQueue"
	// represents the max size of the preemption queue
	_maxReservationQueueSize = 10000
	// reservation queue name
	_reservationQueue = "reservatiom-queue"
	// number of completed reservations to process
	_completedReservations = 1
)

// Reserver represents a placement engine's reservation module
// It gets all the hosts based on filter passed to host manager
// it chooses the random host from the list and call reserve the
// choosen host based on the task.
type Reserver interface {
	// Adding daemon interface for Reserver
	async.Daemon

	// GetReservationQueue returns the reservation queue
	GetReservationQueue() queue.Queue

	// Reserve reserves the task to host in hostmanager
	Reserve(ctx context.Context) (time.Duration, error)

	// EnqueueReservation enqueues the hostsvc.reservation to
	// the reservation queue
	EnqueueReservation(reservation *hostsvc.Reservation) error

	// GetCompletedReservation gets the completed tasks with offers
	// by that placement can be created
	GetCompletetedReservation(ctx context.Context) ([]*hostsvc.CompletedReservation, error)
}

// reserver is the struct which impelements Reserver interface
type reserver struct {
	lock sync.Mutex
	// Placement config for the reserver
	config *config.PlacementConfig
	// Placement engine metrics
	metrics *tally_metrics.Metrics
	// hostService for accessing the host manager for getting host list
	// as well as reserving host
	hostService hosts.Service
	// daemon object for making reservecr a daemon process
	daemon async.Daemon
	// reservation queue for getting the tasks from placement engine
	// to make the reservation
	reservationQueue queue.Queue
	// completedreservation queue
	completedReservationQueue queue.Queue
	// task-> reservation mapping
	reservations map[string][]*models.Host
	// tasks map indexed by taskiD
	tasks map[string]*resmgr.Task
}

// NewReserver creates a new reserver which gets the tasks from the reservationQueue
// and based on the requirements from the task get the hosts list and randomly choose
// the host and make the reservation on that host for the task.
func NewReserver(
	metrics *tally_metrics.Metrics,
	cfg *config.PlacementConfig,
	hostsService hosts.Service) Reserver {
	reserver := &reserver{
		config:      cfg,
		hostService: hostsService,
		metrics:     metrics,
	}
	reserver.daemon = async.NewDaemon("Placement Engine Reserver", reserver)
	reserver.reservationQueue = queue.NewQueue(
		_reservationQueue,
		reflect.TypeOf(resmgr.Task{}), _maxReservationQueueSize)
	reserver.completedReservationQueue = queue.NewQueue(
		_completedReservationQueue,
		reflect.TypeOf(hostsvc.CompletedReservation{}), _maxReservationQueueSize)
	reserver.reservations = make(map[string][]*models.Host)
	reserver.tasks = make(map[string]*resmgr.Task)
	return reserver
}

// Start method starts the daemon process
func (r *reserver) Start() {
	r.daemon.Start()
	r.metrics.Running.Update(1)
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
		err = r.findCompletedReservation(ctx)
		if err != nil {
			log.WithError(err).Info("error finding resrevation")
		}
		timer.Reset(delay)
	}
}

// Stop methis will stop the daemon process.
func (r *reserver) Stop() {
	r.daemon.Stop()
	r.metrics.Running.Update(0)
}

// Reserve method is being called from Run method
// This method does following steps
//   1. Get Tasks from the reservation queue
//   2. Find the hosts list from hostmanager matching filter
//   3. choose one random host from the list
//   4. reserve the host in host manager
func (r *reserver) Reserve(ctx context.Context) (time.Duration, error) {
	// Get Tasks from the reservation queue
	item, err := r.reservationQueue.Dequeue(1 * time.Second)
	if err != nil {
		return _noTasksTimeoutPenalty, errors.New("No items in reservation queue")
	}
	task, ok := item.(*resmgr.Task)
	if !ok || task.GetId() == nil {
		return _noTasksTimeoutPenalty, fmt.Errorf("Not a valid task %s", task.GetId())
	}
	// storing the tasks
	r.tasks[task.GetId().Value] = task

	hostFilter := r.getHostFilter(task)
	// Find the hosts list from hostmanager matching filter
	hosts, err := r.hostService.GetHosts(ctx, task, hostFilter)
	if err != nil {
		log.WithFields(log.Fields{
			"host_filter": hostFilter,
			"task":        task.Id,
		}).Info("Couldn't aquire hosts for task")
		return _noHostsTimeoutPenalty, err
	}

	var hostToReserve []*models.Host
	// choose one random host from the list
	hostToReserve = append(hostToReserve, r.findHost(hosts))
	// reserve the host in host manager
	err = r.hostService.ReserveHost(ctx, hostToReserve, task)
	if err != nil {
		log.WithFields(log.Fields{
			"host": hostToReserve[0].GetHost().Hostname,
			"task": task.Id.Value,
		}).Info("Host could not be reserved")
		return _noHostsTimeoutPenalty, err
	}
	//Updating the task to hosts map
	r.reservations[task.GetId().Value] = hostToReserve
	return time.Duration(0), nil
}

// findHost randomly chooses the number of hosts and then
// out of those hosts choose the one which have lowest numbeer
// of tasks running
func (r *reserver) findHost(hosts []*models.Host) *models.Host {
	lenRandomHosts := _randomizedHosts
	lenHosts := len(hosts)
	if lenRandomHosts > lenHosts {
		lenRandomHosts = lenHosts
	}
	randomHosts := make([]*models.Host, lenRandomHosts)
	for i := 0; i < lenRandomHosts; i++ {
		randomHosts[i] = hosts[random(0, lenHosts)+0]
	}
	return r.findHostWithMinTasks(randomHosts)
}

// findHostWithMinTasks returns the host which has the minimun running task
// from the list of hosts provided
func (r *reserver) findHostWithMinTasks(hosts []*models.Host) *models.Host {
	min := taskLen(hosts[0])
	minindex := 0
	for i, host := range hosts {
		if min >= taskLen(host) {
			min = taskLen(host)
			minindex = i
		}
	}
	return hosts[minindex]
}

func taskLen(host *models.Host) int {
	if host.GetTasks() == nil {
		return 0
	}
	return len(host.GetTasks())
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func (r *reserver) getHostFilter(task *resmgr.Task) *hostsvc.HostFilter {
	result := &hostsvc.HostFilter{
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum:  task.Resource,
			NumPorts: task.NumPorts,
		},
	}
	if constraint := task.Constraint; constraint != nil {
		result.SchedulingConstraint = constraint
	}
	return result
}

// GetReservationQueue gets the reszervation queue
func (r *reserver) GetReservationQueue() queue.Queue {
	return r.reservationQueue
}

func (r *reserver) GetCompletetedReservation(ctx context.Context) ([]*hostsvc.CompletedReservation, error) {
	var reservations []*hostsvc.CompletedReservation
	var err error
	var item interface{}
	for i := 0; i < _completedReservations; i++ {
		item, err = r.completedReservationQueue.Dequeue(100 * time.Millisecond)
		if err != nil {
			break
		}
		res, ok := item.(*hostsvc.CompletedReservation)
		if !ok {
			continue
		}
		reservations = append(reservations, res)
	}
	return reservations, err
}

// findCompletedReservation finds out the completed reservations from
// hosts service and if found enqueue them into completed reservation
// queue by that handler can create placements out of them
func (r *reserver) findCompletedReservation(ctx context.Context) error {
	// Call hosts service to find the reservation
	reservations, err := r.hostService.GetCompletedReservation(ctx)
	if err != nil {
		return err
	}
	if len(reservations) == 0 {
		return errors.New("no completed reservations found")
	}
	// Taking a lock here on reserver
	r.lock.Lock()
	defer r.lock.Unlock()
	// found the valid reservations
	for _, res := range reservations {
		// Check if reservation is succedded or not
		// by looking at the offers length
		// if offers length is zero that means
		// we need to reserve
		if len(res.HostOffers) == 0 {
			err := r.reserveAgain(res.GetTask())
			if err != nil {
				log.WithError(err).
					Errorf("task %s could not be reserved, "+
						"dropping it ", res.GetTask().GetId().Value)
			}
			continue
		}
		// Valid completed reservation found
		err := r.completedReservationQueue.Enqueue(res)
		if err != nil {
			// Assuming placing timeout in hostmanager will
			// make the host available
			log.WithError(err).WithFields(log.Fields{
				"completed_reservation": res,
			}).Errorf("task %s could not be send for placement, "+
				"dropping it ", res.GetTask().GetId().Value)
			continue
		}
		// clean the completed reservation
		r.cleanReservation(res.GetTask())
	}
	return nil
}

func (r *reserver) reserveAgain(task *resmgr.Task) error {
	// cleanig the reservation
	r.cleanReservation(task)
	// enquing the task again for the reservation
	err := r.GetReservationQueue().Enqueue(task)
	if err != nil {
		return err
	}
	return nil
}

func (r *reserver) cleanReservation(task *resmgr.Task) {
	if task != nil {
		delete(r.reservations, task.GetId().Value)
		delete(r.tasks, task.GetId().Value)
	}
}

func (r *reserver) EnqueueReservation(reservation *hostsvc.Reservation) error {
	if reservation == nil {
		return errors.New("invalid reservation")
	}
	err := r.GetReservationQueue().Enqueue(reservation)
	if err != nil {
		return err
	}
	return nil
}
