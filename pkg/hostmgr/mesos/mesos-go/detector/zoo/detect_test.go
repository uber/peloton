package zoo_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector"
	mock_zkdetector "github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector/zoo/mock"
	util "github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/mesosutil"

	. "github.com/uber/peloton/pkg/hostmgr/mesos/mesos-go/detector/zoo"
)

const (
	zkurl        = "zk://127.0.0.1:2181/mesos"
	zkurl_bad    = "zk://127.0.0.1:2181"
	test_zk_path = "/test"
)

func newTestMainInfo(id int) []byte {
	miPb := util.NewMainInfo(fmt.Sprintf("main(%d)@localhost:5050", id), 123456789, 400)
	data, err := proto.Marshal(miPb)
	if err != nil {
		panic(err)
	}
	return data
}

func TestMainDetectorChildrenChanged(t *testing.T) {
	var (
		path         = test_zk_path
		snapDetected = make(chan struct{})

		bf = func(client ZKInterface, done <-chan struct{}) (ZKInterface, error) {
			if client == nil {
				log.Info("bootstrapping detector")
				defer log.Info("bootstrapping detector ..finished")

				mocked, _, errs := mock_zkdetector.NewClient(test_zk_path, "info_0", "info_5", "info_10")
				client = mocked

				mocked.On("Data", fmt.Sprintf("%s/info_0", path)).Return(newTestMainInfo(0), nil)

				// wait for the first child snapshot to be processed before signaling end-of-watch
				// (which is signalled by closing errs).
				go func() {
					defer close(errs)
					select {
					case <-snapDetected:
					case <-done:
						t.Errorf("detector died before child snapshot")
					}
				}()
			}
			return client, nil
		}
		called     = 0
		lostMain = make(chan struct{})
		md, err    = NewMainDetector(zkurl, MinCyclePeriod(10*time.Millisecond), Bootstrap(bf))
	)
	defer md.Cancel()
	assert.NoError(t, err)

	const expectedLeader = "main(0)@localhost:5050"
	err = md.Detect(detector.OnMainChanged(func(main *mesos.MainInfo) {
		//expect 2 calls in sequence: the first setting a main
		//and the second clearing it
		switch called++; called {
		case 1:
			defer close(snapDetected)
			assert.NotNil(t, main)
			assert.Equal(t, expectedLeader, main.GetId())
		case 2:
			md.Cancel()
			defer close(lostMain)
			assert.Nil(t, main)
		default:
			t.Errorf("unexpected notification call attempt %d", called)
		}
	}))
	assert.NoError(t, err)

	fatalOn(t, 10*time.Second, lostMain, "Waited too long for lost main")

	select {
	case <-md.Done():
		assert.Equal(t, 2, called, "expected 2 detection callbacks instead of %d", called)
	case <-time.After(time.Second * 10):
		panic("Waited too long for detector shutdown...")
	}
}

// single connector instance, it's internal connection to zk is flappy
func TestMainDetectorFlappyConnectionState(t *testing.T) {
	const ITERATIONS = 3
	var (
		path = test_zk_path

		bf = func(client ZKInterface, _ <-chan struct{}) (ZKInterface, error) {
			if client == nil {
				log.Info("bootstrapping detector")
				defer log.Info("bootstrapping detector ..finished")

				children := []string{"info_0", "info_5", "info_10"}
				mocked, snaps, errs := mock_zkdetector.NewClient(test_zk_path, children...)
				client = mocked

				mocked.On("Data", fmt.Sprintf("%s/info_0", path)).Return(newTestMainInfo(0), nil)

				// the first snapshot will be sent immediately and the detector will be awaiting en event.
				// cycle through some connected/disconnected events but maintain the same snapshot
				go func() {
					defer close(errs)
					for attempt := 0; attempt < ITERATIONS; attempt++ {
						// send an error, should cause the detector to re-issue a watch
						errs <- zk.ErrSessionExpired
						// the detection loop issues another watch, so send it a snapshot..
						// send another snapshot
						snaps <- children
					}
				}()
			}
			return client, nil
		}
		called     = 0
		lostMain = make(chan struct{})
		wg         sync.WaitGroup
		md, err    = NewMainDetector(zkurl, MinCyclePeriod(10*time.Millisecond), Bootstrap(bf))
	)
	defer md.Cancel()
	assert.NoError(t, err)

	wg.Add(1 + ITERATIONS) // +1 for the initial snapshot that's sent for the first watch

	const EXPECTED_CALLS = (ITERATIONS * 2) + 2 // +1 for initial snapshot, +1 for final lost-leader (close(errs))
	err = md.Detect(detector.OnMainChanged(func(main *mesos.MainInfo) {
		called++
		log.Infof("detector invoked: called %d", called)
		switch {
		case called < EXPECTED_CALLS:
			if main != nil {
				wg.Done()
				assert.Equal(t, main.GetId(), "main(0)@localhost:5050")
			}
		case called == EXPECTED_CALLS:
			md.Cancel()
			defer close(lostMain)
			assert.Nil(t, main)
		default:
			t.Errorf("unexpected notification call attempt %d", called)
		}
	}))
	assert.NoError(t, err)

	fatalAfter(t, 10*time.Second, wg.Wait, "Waited too long for new-main alerts")
	fatalOn(t, 3*time.Second, lostMain, "Waited too long for lost main")

	select {
	case <-md.Done():
		assert.Equal(t, EXPECTED_CALLS, called, "expected %d detection callbacks instead of %d", EXPECTED_CALLS, called)
	case <-time.After(time.Second * 10):
		panic("Waited too long for detector shutdown...")
	}
}

func TestMainDetector_multipleLeadershipChanges(t *testing.T) {
	var (
		leadershipChanges = [][]string{
			{"info_014", "info_010", "info_005"},
			{"info_005", "info_004", "info_022"},
			{}, // indicates no main
			{"info_017", "info_099", "info_200"},
		}

		ITERATIONS = len(leadershipChanges)
		// +1 for initial snapshot, +1 for final lost-leader (close(errs))
		EXPECTED_CALLS = (ITERATIONS + 2)
		path           = test_zk_path

		bf = func(client ZKInterface, _ <-chan struct{}) (ZKInterface, error) {
			if client == nil {
				log.Info("bootstrapping detector")
				defer log.Info("bootstrapping detector ..finished")

				children := []string{"info_0", "info_5", "info_10"}
				mocked, snaps, errs := mock_zkdetector.NewClient(test_zk_path, children...)
				client = mocked

				mocked.On("Data", fmt.Sprintf("%s/info_0", path)).Return(newTestMainInfo(0), nil)
				mocked.On("Data", fmt.Sprintf("%s/info_005", path)).Return(newTestMainInfo(5), nil)
				mocked.On("Data", fmt.Sprintf("%s/info_004", path)).Return(newTestMainInfo(4), nil)
				mocked.On("Data", fmt.Sprintf("%s/info_017", path)).Return(newTestMainInfo(17), nil)

				// the first snapshot will be sent immediately and the detector will be awaiting en event.
				// cycle through some connected/disconnected events but maintain the same snapshot
				go func() {
					defer close(errs)
					for attempt := 0; attempt < ITERATIONS; attempt++ {
						snaps <- leadershipChanges[attempt]
					}
				}()
			}
			return client, nil
		}

		called          = 0
		lostMain      = make(chan struct{})
		expectedLeaders = []int{0, 5, 4, 17}
		leaderIdx       = 0
		wg              sync.WaitGroup
		md, err         = NewMainDetector(zkurl, MinCyclePeriod(10*time.Millisecond), Bootstrap(bf))
	)
	defer md.Cancel()
	assert.NoError(t, err)

	wg.Add(ITERATIONS) // +1 for the initial snapshot that's sent for the first watch, -1 because set 3 is empty
	err = md.Detect(detector.OnMainChanged(func(main *mesos.MainInfo) {
		called++
		log.Infof("detector invoked: called %d", called)
		switch {
		case called < EXPECTED_CALLS:
			if main != nil {
				expectedLeader := fmt.Sprintf("main(%d)@localhost:5050", expectedLeaders[leaderIdx])
				assert.Equal(t, expectedLeader, main.GetId())
				leaderIdx++
				wg.Done()
			}
		case called == EXPECTED_CALLS:
			md.Cancel()
			defer close(lostMain)
			assert.Nil(t, main)
		default:
			t.Errorf("unexpected notification call attempt %d", called)
		}
	}))
	assert.NoError(t, err)

	fatalAfter(t, 10*time.Second, wg.Wait, "Waited too long for new-main alerts")
	fatalOn(t, 3*time.Second, lostMain, "Waited too long for lost main")

	select {
	case <-md.Done():
		assert.Equal(t, EXPECTED_CALLS, called, "expected %d detection callbacks instead of %d", EXPECTED_CALLS, called)
	case <-time.After(time.Second * 10):
		panic("Waited too long for detector shutdown...")
	}
}

func afterFunc(f func()) <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		f()
	}()
	return ch
}

func fatalAfter(t *testing.T, d time.Duration, f func(), msg string, args ...interface{}) {
	fatalOn(t, d, afterFunc(f), msg, args...)
}

func fatalOn(t *testing.T, d time.Duration, ch <-chan struct{}, msg string, args ...interface{}) {
	select {
	case <-ch:
		return
	case <-time.After(d):
		// check for a tie
		select {
		case <-ch:
			return
		default:
			t.Fatalf(msg, args...)
		}
	}
}

/* TODO(jdef) refactor this to work with the new ZKInterface
func TestNotifyAllMains(t *testing.T) {
	c, err := newClient(test_zk_hosts, test_zk_path)
	assert.NoError(t, err)

	childEvents := make(chan zk.Event, 5)
	connector := NewMockConnector()

	c.setFactory(asFactory(func() (Connector, <-chan zk.Event, error) {
		sessionEvents := make(chan zk.Event, 1)
		sessionEvents <- zk.Event{
			Type:  zk.EventSession,
			State: zk.StateConnected,
		}
		return connector, sessionEvents, nil
	}))

	md, err := NewMainDetector(zkurl)
	defer md.Cancel()

	assert.NoError(t, err)

	c.errorHandler = ErrorHandler(func(c *Client, e error) {
		t.Errorf("unexpected error: %v", e)
	})
	md.Client = c

	listener := &allMainsListener{}

	//-- expect primer
	var primer sync.WaitGroup
	ignoreArgs := func(f func()) func(mock.Arguments) {
		primer.Add(1)
		return func(_ mock.Arguments) {
			f()
		}
	}
	connector.On("Children", test_zk_path).Return([]string{}, &zk.Stat{}, nil).Run(ignoreArgs(primer.Done)).Once()
	listener.On("UpdatedMains", []*mesos.MainInfo{}).Return().Run(ignoreArgs(primer.Done)).Once()
	connector.On("ChildrenW", test_zk_path).Return([]string{test_zk_path}, &zk.Stat{}, (<-chan zk.Event)(childEvents), nil).Run(ignoreArgs(primer.Done)).Once()
	md.Detect(listener)
	fatalAfter(t, 3*time.Second, primer.Wait, "timed out waiting for detection primer")

	listener.AssertExpectations(t)
	connector.AssertExpectations(t)

	//-- test membership changes
	type expectedGets struct {
		info []byte
		err  error
	}
	tt := []struct {
		zkEntry   []string
		gets      []expectedGets
		leaderIdx int
	}{
		{[]string{"info_004"}, []expectedGets{{newTestMainInfo(1), nil}}, 0},
		{[]string{"info_007", "info_005", "info_006"}, []expectedGets{{newTestMainInfo(2), nil}, {newTestMainInfo(3), nil}, {newTestMainInfo(4), nil}}, 1},
		{nil, nil, -1},
	}
	for j, tc := range tt {
		// expectations
		var tcwait sync.WaitGroup
		ignoreArgs = func(f func()) func(mock.Arguments) {
			tcwait.Add(1)
			return func(_ mock.Arguments) {
				f()
			}
		}

		expectedInfos := []*mesos.MainInfo{}
		for i, zke := range tc.zkEntry {
			connector.On("Get", fmt.Sprintf("%s/%s", test_zk_path, zke)).Return(tc.gets[i].info, &zk.Stat{}, tc.gets[i].err).Run(ignoreArgs(tcwait.Done)).Once()
			mainInfo := &mesos.MainInfo{}
			err = proto.Unmarshal(tc.gets[i].info, mainInfo)
			if err != nil {
				t.Fatalf("failed to unmarshall MainInfo data: %v", err)
			}
			expectedInfos = append(expectedInfos, mainInfo)
		}
		if len(tc.zkEntry) > 0 {
			connector.On("Get", fmt.Sprintf("%s/%s", test_zk_path, tc.zkEntry[tc.leaderIdx])).Return(
				tc.gets[tc.leaderIdx].info, &zk.Stat{}, tc.gets[tc.leaderIdx].err).Run(ignoreArgs(tcwait.Done)).Once()
		}
		connector.On("Children", test_zk_path).Return(tc.zkEntry, &zk.Stat{}, nil).Run(ignoreArgs(tcwait.Done)).Once()
		listener.On("OnMainChanged", mock.AnythingOfType("*mesosproto.MainInfo")).Return().Run(ignoreArgs(tcwait.Done)).Once()
		listener.On("UpdatedMains", expectedInfos).Return().Run(ignoreArgs(tcwait.Done)).Once()
		connector.On("ChildrenW", test_zk_path).Return([]string{test_zk_path}, &zk.Stat{}, (<-chan zk.Event)(childEvents), nil).Run(ignoreArgs(tcwait.Done)).Once()

		// fire the event that triggers the test case
		childEvents <- zk.Event{
			Type: zk.EventNodeChildrenChanged,
			Path: test_zk_path,
		}

		// allow plenty of time for all the async processing to happen
		fatalAfter(t, 5*time.Second, tcwait.Wait, "timed out waiting for all-mains test case %d", j+1)
		listener.AssertExpectations(t)
		connector.AssertExpectations(t)
	}

	connector.On("Close").Return(nil)
}
*/
