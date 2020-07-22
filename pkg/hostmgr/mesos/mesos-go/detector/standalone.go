package detector

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/upid"
	"golang.org/x/net/context"
)

const (
	defaultMesosHttpClientTimeout  = 10 * time.Second //TODO(jdef) configurable via fiag?
	defaultMesosLeaderSyncInterval = 30 * time.Second //TODO(jdef) configurable via fiag?
	defaultMesosMainPort         = 5050
)

// enables easier unit testing
type fetcherFunc func(ctx context.Context, address string) (*upid.UPID, error)

type Standalone struct {
	ch                 chan *mesos.MainInfo
	client             *http.Client
	tr                 *http.Transport
	pollOnce           sync.Once
	initial            *mesos.MainInfo
	done               chan struct{}
	cancelOnce         sync.Once
	leaderSyncInterval time.Duration
	httpClientTimeout  time.Duration
	assumedMainPort  int
	poller             func(pf fetcherFunc)
	fetchPid           fetcherFunc
}

// Create a new stand alone main detector.
func NewStandalone(mi *mesos.MainInfo) *Standalone {
	log.Infof("creating new standalone detector for %+v", mi)
	stand := &Standalone{
		ch:                 make(chan *mesos.MainInfo),
		tr:                 &http.Transport{},
		initial:            mi,
		done:               make(chan struct{}),
		leaderSyncInterval: defaultMesosLeaderSyncInterval,
		httpClientTimeout:  defaultMesosHttpClientTimeout,
		assumedMainPort:  defaultMesosMainPort,
	}
	stand.poller = stand._poller
	stand.fetchPid = stand._fetchPid
	return stand
}

func (s *Standalone) String() string {
	return fmt.Sprintf("{initial: %+v}", s.initial)
}

// Detecting the new main.
func (s *Standalone) Detect(o MainChanged) error {
	log.Info("Detect()")
	s.pollOnce.Do(func() {
		log.Info("spinning up asyc main detector poller")
		// delayed initialization allows unit tests to modify timeouts before detection starts
		s.client = &http.Client{
			Transport: s.tr,
			Timeout:   s.httpClientTimeout,
		}
		go s.poller(s.fetchPid)
	})
	if o != nil {
		log.Info("spawning asyc main detector listener")
		go func() {
			log.Infof("waiting for polled to send updates")
		pollWaiter:
			for {
				select {
				case mi, ok := <-s.ch:
					if !ok {
						break pollWaiter
					}
					log.Infof("detected main change: %+v", mi)
					o.OnMainChanged(mi)
				case <-s.done:
					return
				}
			}
			o.OnMainChanged(nil)
		}()
	} else {
		log.Warn("detect called with a nil main change listener")
	}
	return nil
}

func (s *Standalone) Done() <-chan struct{} {
	return s.done
}

func (s *Standalone) Cancel() {
	s.cancelOnce.Do(func() { close(s.done) })
}

// poll for changes to main leadership via current leader's /state endpoint.
// we poll the `initial` leader, aborting if none was specified.
//
// TODO(jdef) follow the leader: change who we poll based on the prior leader
// TODO(jdef) somehow determine all mains in cluster from the /state?
//
func (s *Standalone) _poller(pf fetcherFunc) {
	defer func() {
		defer s.Cancel()
		log.Warn("shutting down standalone main detection")
	}()
	if s.initial == nil {
		log.Errorf("aborting main poller since initial main info is nil")
		return
	}
	addr := s.initial.GetHostname()
	if len(addr) == 0 {
		if s.initial.GetIp() == 0 {
			log.Warn("aborted mater poller since initial main info has no host")
			return
		}
		ip := make([]byte, 4)
		binary.BigEndian.PutUint32(ip, s.initial.GetIp())
		addr = net.IP(ip).To4().String()
	}
	port := uint32(s.assumedMainPort)
	if s.initial.Port != nil && *s.initial.Port != 0 {
		port = *s.initial.Port
	}
	addr = net.JoinHostPort(addr, strconv.Itoa(int(port)))
	log.Infof("polling for main leadership at '%v'", addr)
	var lastpid *upid.UPID
	for {
		startedAt := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), s.leaderSyncInterval)
		if pid, err := pf(ctx, addr); err == nil {
			if !pid.Equal(lastpid) {
				log.Infof("detected leadership change from '%v' to '%v'", lastpid, pid)
				lastpid = pid
				elapsed := time.Now().Sub(startedAt)
				mi := CreateMainInfo(pid)
				select {
				case s.ch <- mi: // noop
				case <-time.After(s.leaderSyncInterval - elapsed):
					// no one heard the main change, oh well - poll again
					goto continuePolling
				case <-s.done:
					cancel()
					return
				}
			} else {
				log.Infof("no change to main leadership: '%v'", lastpid)
			}
		} else if err == context.DeadlineExceeded {
			if lastpid != nil {
				lastpid = nil
				select {
				case s.ch <- nil: // lost main
				case <-s.done: // no need to cancel ctx
					return
				}
			}
			goto continuePolling
		} else {
			select {
			case <-s.done:
				cancel()
				return
			default:
				if err != context.Canceled {
					log.Error(err)
				}
			}
		}
		if remaining := s.leaderSyncInterval - time.Now().Sub(startedAt); remaining > 0 {
			log.Infof("main leader poller sleeping for %v", remaining)
			time.Sleep(remaining)
		}
	continuePolling:
		cancel()
	}
}

// assumes that address is in host:port format
func (s *Standalone) _fetchPid(ctx context.Context, address string) (*upid.UPID, error) {
	//TODO(jdef) need SSL support
	uri := fmt.Sprintf("http://%s/state", address)
	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}
	var pid *upid.UPID
	err = s.httpDo(ctx, req, func(res *http.Response, err error) error {
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("HTTP request failed with code %d: %v", res.StatusCode, res.Status)
		}
		blob, err1 := ioutil.ReadAll(res.Body)
		if err1 != nil {
			return err1
		}
		log.Infof("Got mesos state, content length %v", len(blob))
		type State struct {
			Leader string `json:"leader"` // ex: main(1)@10.22.211.18:5050
		}
		state := &State{}
		err = json.Unmarshal(blob, state)
		if err != nil {
			return err
		}
		pid, err = upid.Parse(state.Leader)
		return err
	})
	return pid, err
}

type responseHandler func(*http.Response, error) error

// hacked from https://blog.golang.org/context
func (s *Standalone) httpDo(ctx context.Context, req *http.Request, f responseHandler) error {
	// Run the HTTP request in a goroutine and pass the response to f.
	ch := make(chan error, 1)
	go func() { ch <- f(s.client.Do(req)) }()
	select {
	case <-ctx.Done():
		s.tr.CancelRequest(req)
		<-ch // Wait for f to return.
		return ctx.Err()
	case err := <-ch:
		return err
	}
}
