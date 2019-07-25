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

package models_v0

import (
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/v0"
)

// ToMimirEntity converts the assignment into a mimir entity.
func (a *Assignment) ToMimirEntity() *placement.Entity {
	data := a.GetTask().Data()
	if data == nil {
		entity := mimir_v0.TaskToEntity(a.GetTask().GetTask(), false)
		a.GetTask().SetData(entity)
		data = entity
	}
	entity := data.(*placement.Entity)
	return entity
}

// ToMimirGroup converts the HostOffers object into a mimir group.
func (o *HostOffers) ToMimirGroup() *placement.Group {
	data := o.Data()
	if data == nil {
		group := mimir_v0.OfferToGroup(o.GetOffer())
		entities := placement.Entities{}
		for _, task := range o.GetTasks() {
			entity := mimir_v0.TaskToEntity(task, true)
			entities.Add(entity)
		}
		group.Entities = entities
		group.Update()
		o.SetData(group)
		data = group
	}
	group := data.(*placement.Group)
	return group
}
