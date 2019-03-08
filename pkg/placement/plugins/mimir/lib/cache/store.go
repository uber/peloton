// @generated AUTO GENERATED - DO NOT EDIT! 117d51fa2854b0184adc875246a35929bbbf0a91

// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cache

import (
	"sort"
	"sync"
	"time"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Store will store placement groups and entities for use by a worker node.
type Store interface {
	// Find will return the group with the given name if it exists, else it will return nil.
	Find(name string) (group *placement.Group)

	// All returns all groups in the store in sorted order by name.
	All() (all []*placement.Group)

	// Search finds all groups that have a label or relation that match the given pattern. The source determines if
	// the pattern should match a label or a relation. The resulting groups will be in sorted order by name.
	Search(pattern *labels.Label, source Source) (groups []*placement.Group)

	// Reserved returns all entities that are reserved.
	Reserved() (reservations []*placement.Reservation)

	// Next returns the next entity and the group it is place on in the store. The entities are returned in the
	// following order; lexicographically in the order of the group names, and it case of ties lexicographically in
	// the order of the entity names.
	Next() (group *placement.Group, entity *placement.Entity)

	// Update will add the groups if they do not already exist - existence is checked by the group name. If the group
	// already exists it will just update it while retaining the groups reservations.
	Update(now time.Time, groups ...*placement.Group)

	// Prune will remove any expired reservations from all groups, it will also remove any expired groups.
	Prune(now time.Time, groupMaxAge, reservationMaxAge time.Duration)
}

// NewStore will create a new store.
func NewStore() Store {
	return &store{
		groups: map[string]*groupMeta{},
	}
}

type sortedGroups []*placement.Group

func (groups sortedGroups) Len() int {
	return len(groups)
}

func (groups sortedGroups) Less(i, j int) bool {
	return groups[i].Name < groups[j].Name
}

func (groups sortedGroups) Swap(i, j int) {
	groups[i], groups[j] = groups[j], groups[i]
}

type groupMeta struct {
	creation time.Time
	group    *placement.Group
}

type store struct {
	groups  map[string]*groupMeta
	current *string
	lock    sync.RWMutex
}

func (store *store) Find(name string) *placement.Group {
	defer store.lock.RUnlock()
	store.lock.RLock()

	meta := store.groups[name]
	if meta == nil {
		return nil
	}
	return meta.group
}

func (store *store) Search(pattern *labels.Label, source Source) []*placement.Group {
	defer store.lock.RUnlock()
	store.lock.RLock()

	var result sortedGroups
	for _, meta := range store.groups {
		switch source {
		case Label:
			if len(meta.group.Labels.Find(pattern)) > 0 {
				result = append(result, meta.group)
			}
		case Relation:
			if len(meta.group.Relations.Find(pattern)) > 0 {
				result = append(result, meta.group)
			}
		}
	}
	sort.Sort(result)
	return result
}

func (store *store) All() []*placement.Group {
	defer store.lock.RUnlock()
	store.lock.RLock()

	var result sortedGroups
	for _, meta := range store.groups {
		result = append(result, meta.group)
	}
	sort.Sort(result)
	return result
}

func (store *store) Reserved() []*placement.Reservation {
	defer store.lock.RUnlock()
	store.lock.RLock()
	var result pairList
	for _, meta := range store.groups {
		group := meta.group
		for _, entity := range group.Entities {
			if entity.Reservation.IsReserved {
				result = append(result, &placement.Reservation{
					Entity: entity,
					Group:  group,
				})
			}
		}
	}
	sort.Sort(result)
	return result
}

type pairList []*placement.Reservation

func (list pairList) Len() int {
	return len(list)
}

func (list pairList) Less(i, j int) bool {
	return list.pair(i) < list.pair(j)
}

func (list pairList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list pairList) pair(i int) string {
	return list[i].Group.Name + list[i].Entity.Name
}

func (store *store) Next() (*placement.Group, *placement.Entity) {
	defer store.lock.RUnlock()
	store.lock.RLock()

	if len(store.groups) == 0 {
		return nil, nil
	}

	// Collect a list of all pairs of group and entity.
	var pairs pairList
	for _, meta := range store.groups {
		group := meta.group
		if len(group.Entities) == 0 {
			continue
		}
		for _, entity := range group.Entities {
			pairs = append(pairs, &placement.Reservation{
				Group:  group,
				Entity: entity,
			})
		}
	}
	// Sort the pairs by group name and entity name.
	sort.Sort(pairs)
	// Find the successor of the current pair.
	currentPair := ""
	if store.current != nil {
		currentPair = *store.current
	}
	nextIndex := sort.Search(len(pairs), func(i int) bool {
		return currentPair < pairs.pair(i)
	})
	// If there are no successor of the current pair we wrap around and return the first pair.
	if nextIndex >= len(pairs) {
		nextIndex = 0
	}
	// Update the current pair and return the corresponding group and entity.
	nextPair := pairs[nextIndex]
	next := pairs.pair(nextIndex)
	store.current = &next
	return nextPair.Group, nextPair.Entity
}

func (store *store) Update(now time.Time, groups ...*placement.Group) {
	defer store.lock.Unlock()
	store.lock.Lock()

	for _, group := range groups {
		meta, exists := store.groups[group.Name]
		if exists {
			oldGroup := meta.group
			for _, entity := range oldGroup.Entities {
				if entity.Reservation.IsReserved {
					group.Entities.Add(entity)
				}
			}
			group.Update()
		}
		store.groups[group.Name] = &groupMeta{
			group:    group,
			creation: now,
		}
	}
}

func (store *store) Prune(now time.Time, groupMaxAge, reservationMaxAge time.Duration) {
	defer store.lock.Unlock()
	store.lock.Lock()

	var removedGroups []string
	for _, meta := range store.groups {
		group := meta.group
		if age := now.Sub(meta.creation); age >= groupMaxAge {
			removedGroups = append(removedGroups, group.Name)
		}
		removedEntities := make(placement.Entities)
		for _, entity := range group.Entities {
			if age := now.Sub(entity.Reservation.Creation); entity.Reservation.IsReserved && age >= reservationMaxAge {
				removedEntities.Add(entity)
			}
		}
		for _, entity := range removedEntities {
			group.Entities.Remove(entity)
		}
	}
	for _, groupName := range removedGroups {
		delete(store.groups, groupName)
	}
}
