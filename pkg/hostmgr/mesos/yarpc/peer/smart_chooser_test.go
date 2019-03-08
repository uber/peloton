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

package peer

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/uber/peloton/pkg/common/leader"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"
)

type fakeObserver struct {
	running bool
}

func (*fakeObserver) CurrentLeader() (string, error) {
	return "", nil
}

func (o *fakeObserver) Start() error {
	o.running = true
	return nil
}

func (o *fakeObserver) Stop() {
	o.running = false
}

type SmartChooserTestSuite struct {
	suite.Suite
	chooser  Chooser
	observer *fakeObserver
}

func TestSmartChooser(t *testing.T) {
	suite.Run(t, new(SmartChooserTestSuite))
}

func (suite *SmartChooserTestSuite) SetupTest() {
	cfg := leader.ElectionConfig{ZKServers: []string{"localhost"},
		Root: "/peloton/testrole"}
	ts := grpc.NewTransport()
	ch, err := NewSmartChooser(cfg,
		tally.NewTestScope("test", nil), "testrole", ts)
	suite.Nil(err)
	suite.observer = &fakeObserver{}
	ch.(*smartChooser).observer = suite.observer
	suite.chooser = ch
}

func (suite *SmartChooserTestSuite) TearDownTest() {
}

// Tests repeated starting/stopping
func (suite *SmartChooserTestSuite) TestStartStop() {
	suite.chooser.Start()
	suite.True(suite.chooser.IsRunning())
	suite.True(suite.observer.running)

	suite.Error(suite.chooser.Start())

	suite.chooser.Stop()
	suite.False(suite.chooser.IsRunning())
	suite.False(suite.observer.running)

	suite.Error(suite.chooser.Stop())
}

// Tests creating chooser with invalid election config
func (suite *SmartChooserTestSuite) TestBadElectionConfig() {
	cfg := leader.ElectionConfig{}
	ts := grpc.NewTransport()
	_, err := NewSmartChooser(cfg,
		tally.NewTestScope("test", nil), "testrole", ts)
	suite.Error(err)
}

// Tests choosing a peer before and after update
func (suite *SmartChooserTestSuite) TestUpdateAndChoosePeer() {
	testcases := []struct {
		leaderIP   string
		leaderPort int
	}{
		{},
		{leaderIP: "127.0.0.1", leaderPort: 9123},
		{leaderIP: "127.0.0.127", leaderPort: 80},
	}
	for _, tc := range testcases {
		expectedPeerID := ""
		if tc.leaderIP != "" {
			expectedPeerID = fmt.Sprintf("%s:%d", tc.leaderIP, tc.leaderPort)
			id := leader.ID{IP: tc.leaderIP, GRPCPort: tc.leaderPort}
			leader, _ := json.Marshal(id)
			err := suite.chooser.UpdatePeer(string(leader))
			suite.NoError(err)
		}
		ctx := context.Background()
		req := &transport.Request{}
		p, _, err := suite.chooser.Choose(ctx, req)
		suite.NoError(err)
		if expectedPeerID != "" {
			suite.Equal(expectedPeerID, p.Identifier())
		} else {
			suite.Nil(p)
		}
	}
}

// Tests updating peer with invalid values
func (suite *SmartChooserTestSuite) TestUpdateInvalidPeer() {
	nonJSON := "foo-bar;"
	testcases := []string{nonJSON}

	for _, tc := range testcases {
		suite.Error(suite.chooser.UpdatePeer(tc), tc)
		p, _, _ := suite.chooser.Choose(
			context.Background(), &transport.Request{})
		suite.Nil(p, tc)
	}
}
