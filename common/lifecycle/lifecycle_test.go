package lifecycle

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type LifeCycleTestSuite struct {
	suite.Suite
	lifeCycle LifeCycle
}

func TestLifeCycle(t *testing.T) {
	suite.Run(t, new(LifeCycleTestSuite))
}

func (s *LifeCycleTestSuite) SetupTest() {
	s.lifeCycle = NewLifeCycle()
}

func (s *LifeCycleTestSuite) TestNormalFlow() {
	var testStart sync.WaitGroup
	var testFinish sync.WaitGroup
	testStart.Add(1)
	testFinish.Add(1)

	s.lifeCycle.Start()
	go func() {
		stopCh := s.lifeCycle.StopCh()
		testStart.Done()
		select {
		case <-stopCh:
			s.lifeCycle.StopComplete()
			testFinish.Done()
		}
	}()
	testStart.Wait()
	s.lifeCycle.Stop()
	s.lifeCycle.Wait()
	testFinish.Wait()
}

func (s *LifeCycleTestSuite) TestBroadcastStop() {
	numOfTestGoroutines := 10
	var testStart sync.WaitGroup
	var testFinish sync.WaitGroup
	testStart.Add(numOfTestGoroutines)
	testFinish.Add(numOfTestGoroutines)

	s.lifeCycle.Start()
	for i := 0; i < numOfTestGoroutines; i++ {
		go func() {
			stopCh := s.lifeCycle.StopCh()
			testStart.Done()
			select {
			case <-stopCh:
				testFinish.Done()
			}
		}()
	}
	go func() {
		testFinish.Wait()
		s.lifeCycle.StopComplete()
	}()
	testStart.Wait()
	s.lifeCycle.Stop()
	s.lifeCycle.Wait()
}

func (s *LifeCycleTestSuite) TestUnStartedLifecycleNotBlock() {
	numOfTestGoroutines := 10
	var testStart sync.WaitGroup
	var testFinish sync.WaitGroup
	testStart.Add(numOfTestGoroutines)
	testFinish.Add(numOfTestGoroutines)

	for i := 0; i < numOfTestGoroutines; i++ {
		go func() {
			stopCh := s.lifeCycle.StopCh()
			testStart.Done()
			select {
			case <-stopCh:
				testFinish.Done()
			}
		}()
	}
	testStart.Wait()
	go func() {
		testFinish.Wait()
		s.lifeCycle.StopComplete()
	}()
	s.lifeCycle.Stop()
	s.lifeCycle.Wait()
}

func (s *LifeCycleTestSuite) TestStartAndStop() {
	numOfTestGoroutines := 10
	var testStart sync.WaitGroup
	var testFinish sync.WaitGroup
	testStart.Add(numOfTestGoroutines)
	testFinish.Add(numOfTestGoroutines)

	s.lifeCycle.Start()
	for i := 0; i < numOfTestGoroutines; i++ {
		go func() {
			stopCh := s.lifeCycle.StopCh()
			testStart.Done()
			select {
			case <-stopCh:
				testFinish.Done()
			}
		}()
	}
	testStart.Wait()
	go func() {
		testFinish.Wait()
		s.lifeCycle.StopComplete()
	}()
	s.lifeCycle.Stop()
	s.lifeCycle.Wait()

	testStart.Add(numOfTestGoroutines)
	testFinish.Add(numOfTestGoroutines)

	s.lifeCycle.Start()
	for i := 0; i < numOfTestGoroutines; i++ {
		go func() {
			stopCh := s.lifeCycle.StopCh()
			testStart.Done()
			select {
			case <-stopCh:
				testFinish.Done()
			}
		}()
	}
	testStart.Wait()
	go func() {
		testFinish.Wait()
		s.lifeCycle.StopComplete()
	}()
	s.lifeCycle.Stop()
	s.lifeCycle.Wait()
}

func (s *LifeCycleTestSuite) TestCallStopCompleteBeforeStop() {
	var testFinish sync.WaitGroup
	testFinish.Add(1)

	s.lifeCycle.Start()
	s.lifeCycle.Stop()
	go func() {
		stopCh := s.lifeCycle.StopCh()
		select {
		case <-stopCh:
			s.lifeCycle.StopComplete()
			testFinish.Done()
		}
	}()
	s.lifeCycle.Wait()
	testFinish.Wait()
}
