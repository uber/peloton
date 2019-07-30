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

package procedure

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type LabelManagerTestSuite struct {
	suite.Suite
}

// Test the basic usage of LabelManager.HasLabel
func (suite *LabelManagerTestSuite) TestLabelManagerHasLabelBasic() {
	labelManager := NewLabelManager(&LabelManagerConfig{
		Entries: []*LabelManagerConfigEntry{
			{Procedures: []string{"testService:Procedure1"}, Labels: []string{"label1"}},
			{Procedures: []string{"testService:Procedure2"}, Labels: []string{"label2"}},
			{Procedures: []string{"testService:Procedure3"}, Labels: []string{"label3"}},
		},
	})

	suite.True(labelManager.HasLabel("testService::Procedure1", "label1"))
	suite.True(labelManager.HasLabel("testService::Procedure2", "label2"))
	suite.True(labelManager.HasLabel("testService::Procedure3", "label3"))

	suite.False(labelManager.HasLabel("testService::Procedure1", "label3"))
	suite.False(labelManager.HasLabel("testService::Procedure2", "label1"))
	suite.False(labelManager.HasLabel("testService::Procedure3", "label1"))
}

// Test if LabelManager.HasLabel can deal with entries with multiple label
func (suite *LabelManagerTestSuite) TestLabelManagerHasLabelMultipleLabels() {
	labelManager := NewLabelManager(&LabelManagerConfig{
		Entries: []*LabelManagerConfigEntry{
			{Procedures: []string{"testService:Procedure1"}, Labels: []string{"label1", "label2"}},
			{Procedures: []string{"testService:Procedure1"}, Labels: []string{"label3"}},
		},
	})

	suite.True(labelManager.HasLabel("testService::Procedure1", "label1"))
	suite.True(labelManager.HasLabel("testService::Procedure1", "label2"))
	suite.True(labelManager.HasLabel("testService::Procedure1", "label3"))

	suite.False(labelManager.HasLabel("testService::Procedure1", "label4"))
}

// Test LabelManager.HasLabel in the case of wildcard in service is used
func (suite *LabelManagerTestSuite) TestLabelManagerHasLabelWildcardService() {
	labelManager := NewLabelManager(&LabelManagerConfig{
		Entries: []*LabelManagerConfigEntry{
			{Procedures: []string{"*:Procedure1"}, Labels: []string{"label1", "label2"}},
			{Procedures: []string{"*:Procedure2"}, Labels: []string{"label3", "label4"}},
			{Procedures: []string{"*:Procedure3*"}, Labels: []string{"label5", "label6"}},
		},
	})

	suite.True(labelManager.HasLabel("testService1::Procedure1", "label1"))
	suite.True(labelManager.HasLabel("testService1::Procedure1", "label2"))
	suite.True(labelManager.HasLabel("testService2::Procedure1", "label1"))
	suite.True(labelManager.HasLabel("testService2::Procedure1", "label2"))

	suite.True(labelManager.HasLabel("testService1::Procedure2", "label3"))
	suite.True(labelManager.HasLabel("testService1::Procedure2", "label4"))
	suite.True(labelManager.HasLabel("testService2::Procedure2", "label3"))
	suite.True(labelManager.HasLabel("testService2::Procedure2", "label4"))

	suite.True(labelManager.HasLabel("testService1::Procedure3", "label5"))
	suite.True(labelManager.HasLabel("testService1::Procedure3", "label6"))
	suite.True(labelManager.HasLabel("testService2::Procedure3", "label5"))
	suite.True(labelManager.HasLabel("testService2::Procedure3", "label6"))
	suite.True(labelManager.HasLabel("testService1::Procedure34", "label5"))
	suite.True(labelManager.HasLabel("testService1::Procedure34", "label6"))
	suite.True(labelManager.HasLabel("testService2::Procedure34", "label5"))
	suite.True(labelManager.HasLabel("testService2::Procedure34", "label6"))

	suite.False(labelManager.HasLabel("testService1::Procedure1", "label3"))
	suite.False(labelManager.HasLabel("testService1::Procedure1", "label4"))
	suite.False(labelManager.HasLabel("testService2::Procedure1", "label3"))
	suite.False(labelManager.HasLabel("testService2::Procedure1", "label4"))

	suite.False(labelManager.HasLabel("testService1::Procedure2", "label1"))
	suite.False(labelManager.HasLabel("testService1::Procedure2", "label2"))
	suite.False(labelManager.HasLabel("testService2::Procedure2", "label1"))
	suite.False(labelManager.HasLabel("testService2::Procedure2", "label2"))
}

// Test if LabelManager.HasLabel can deal with entries with multiple label
func (suite *LabelManagerTestSuite) TestLabelManagerHasLabelWildcardMethod() {
	labelManager := NewLabelManager(&LabelManagerConfig{
		Entries: []*LabelManagerConfigEntry{
			{Procedures: []string{"testService1:*"}, Labels: []string{"label1", "label2"}},
			{Procedures: []string{"testService2:Get*"}, Labels: []string{"label1", "label2"}},
		},
	})

	suite.True(labelManager.HasLabel("testService1::Procedure1", "label1"))
	suite.True(labelManager.HasLabel("testService1::Procedure1", "label2"))
	suite.True(labelManager.HasLabel("testService1::Procedure2", "label1"))
	suite.True(labelManager.HasLabel("testService1::Procedure2", "label2"))
	suite.True(labelManager.HasLabel("testService2::Get", "label1"))
	suite.True(labelManager.HasLabel("testService2::Get", "label2"))
	suite.True(labelManager.HasLabel("testService2::Get1", "label1"))
	suite.True(labelManager.HasLabel("testService2::Get2", "label2"))

	suite.False(labelManager.HasLabel("testService2::Procedure1", "label1"))
	suite.False(labelManager.HasLabel("testService2::Procedure1", "label2"))
}

// Test if LabelManager.HasLabel can handle default config with no setting
func (suite *LabelManagerTestSuite) TestLabelManagerDefaultConfig() {
	labelManager := NewLabelManager(&LabelManagerConfig{})

	suite.False(labelManager.HasLabel("testService1::Procedure2", "label1"))
	suite.False(labelManager.HasLabel("testService1::Procedure2", "label2"))
	suite.False(labelManager.HasLabel("testService2::Procedure2", "label1"))
	suite.False(labelManager.HasLabel("testService2::Procedure2", "label2"))
}

func TestLabelManagerTestSuite(t *testing.T) {
	suite.Run(t, new(LabelManagerTestSuite))
}
