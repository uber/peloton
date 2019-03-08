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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"sort"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

func setupStore() Store {
	store := NewStore()
	now := time.Now()

	group1 := placement.NewGroup("group1")
	group1.Labels.Add(labels.NewLabel("sku", "A"))

	entity1 := placement.NewEntity("entity1")
	entity1.Reservation.IsReserved = true
	entity1.Reservation.Creation = now
	entity1.Relations.Add(labels.NewLabel("instance", "A"))
	group1.Entities.Add(entity1)

	group1.Update()

	group2 := placement.NewGroup("group2")
	group2.Labels.Add(labels.NewLabel("sku", "B"))

	entity2 := placement.NewEntity("entity2")
	entity2.Reservation.IsReserved = true
	entity2.Reservation.Creation = now
	entity2.Relations.Add(labels.NewLabel("instance", "A"))
	entity3 := placement.NewEntity("entity3")
	entity3.Relations.Add(labels.NewLabel("instance", "B"))
	group2.Entities.Add(entity2)
	group2.Entities.Add(entity3)

	group2.Update()

	group3 := placement.NewGroup("group3")
	group3.Labels.Add(labels.NewLabel("sku", "B"))
	group3.Update()

	group4 := placement.NewGroup("group4")
	group4.Labels.Add(labels.NewLabel("sku", "C"))

	entity4 := placement.NewEntity("entity4")
	entity4.Relations.Add(labels.NewLabel("instance", "B"))
	group4.Entities.Add(entity4)

	group4.Update()

	store.Update(now, group1)
	store.Update(now, group2)
	store.Update(now, group3)
	store.Update(now, group4)
	return store
}

func TestSortedGroups_Len(t *testing.T) {
	all := setupStore().All()
	sorted := sortedGroups(all)

	assert.Equal(t, len(all), len(sorted))
}

func TestSortedGroups_Less(t *testing.T) {
	sorted := sortedGroups{placement.NewGroup("1"), placement.NewGroup("2")}

	assert.True(t, sorted.Less(0, 1))
	assert.False(t, sorted.Less(1, 0))
}

func TestSortedGroups_Swap(t *testing.T) {
	sorted := sortedGroups{placement.NewGroup("1"), placement.NewGroup("2")}

	assert.True(t, sorted.Less(0, 1))
	sorted.Swap(0, 1)
	assert.True(t, sorted.Less(1, 0))
}

func TestPairList_Len(t *testing.T) {
	pairs := pairList(setupStore().Reserved())

	assert.Equal(t, 2, pairs.Len())
}

func TestPairList_Less(t *testing.T) {
	pairs := pairList(setupStore().Reserved())
	sort.Sort(pairs)

	assert.True(t, pairs.Less(0, 1))
	assert.False(t, pairs.Less(1, 0))
}

func TestPairList_Swap(t *testing.T) {
	pairs := pairList(setupStore().Reserved())
	sort.Sort(pairs)

	assert.True(t, pairs.Less(0, 1))
	pairs.Swap(0, 1)
	assert.True(t, pairs.Less(1, 0))
}

func TestStore_Find(t *testing.T) {
	store := setupStore()
	group := store.Find("group1")

	assert.NotNil(t, group)
	assert.Equal(t, "group1", group.Name)
}

func TestStore_All(t *testing.T) {
	store := setupStore()
	all := store.All()

	assert.NotNil(t, all)
	assert.Equal(t, 4, len(all))
}

func TestStore_Search_find_groups_with_sku_A(t *testing.T) {
	store := setupStore()
	pattern := labels.NewLabel("sku", "A")
	groups := store.Search(pattern, Label)

	assert.NotNil(t, groups)
	assert.Equal(t, 1, len(groups))
	for _, group := range groups {
		assert.True(t, group.Labels.Contains(pattern))
	}
}

func TestStore_Search_find_groups_instance_A(t *testing.T) {
	store := setupStore()
	pattern := labels.NewLabel("instance", "A")
	groups := store.Search(pattern, Relation)

	assert.NotNil(t, groups)
	assert.Equal(t, 2, len(groups))
	for _, group := range groups {
		assert.True(t, group.Relations.Contains(pattern))
	}
}

func TestStore_Reserved(t *testing.T) {
	store := setupStore()
	reserved := store.Reserved()

	assert.NotNil(t, reserved)
	assert.Equal(t, 2, len(reserved))
	assert.Equal(t, "entity1", reserved[0].Entity.Name)
	assert.Equal(t, "group1", reserved[0].Group.Name)
	assert.Equal(t, "entity2", reserved[1].Entity.Name)
	assert.Equal(t, "group2", reserved[1].Group.Name)
}

func TestStore_Next_returns_nil_on_an_empty_store(t *testing.T) {
	store := NewStore()

	group, entity := store.Next()
	assert.Nil(t, group)
	assert.Nil(t, entity)
}

func TestStore_Next_returns_the_same_pair_for_a_store_with_one_entity(t *testing.T) {
	store := setupStore().(*store)
	delete(store.groups, "group2")
	delete(store.groups, "group3")
	delete(store.groups, "group4")

	group1, entity1 := store.Next()
	assert.NotNil(t, group1)
	assert.NotNil(t, entity1)
	assert.Equal(t, "group1", group1.Name)
	assert.Equal(t, "entity1", entity1.Name)

	group1m, entity2 := store.Next()
	assert.NotNil(t, group1m)
	assert.NotNil(t, entity2)
	assert.Equal(t, "group1", group1m.Name)
	assert.Equal(t, "entity1", entity2.Name)
}

func TestStore_Next_removing_the_group_with_the_current_entity_picks_the_next_one(t *testing.T) {
	store := setupStore().(*store)
	group1, entity1 := store.Next()
	assert.NotNil(t, group1)
	assert.NotNil(t, entity1)
	assert.Equal(t, "group1", group1.Name)
	assert.Equal(t, "entity1", entity1.Name)

	group2, entity2 := store.Next()
	assert.NotNil(t, group2)
	assert.NotNil(t, entity2)
	assert.Equal(t, "group2", group2.Name)
	assert.Equal(t, "entity2", entity2.Name)

	delete(store.groups, "group2")

	group4, entity4 := store.Next()
	assert.NotNil(t, group4)
	assert.NotNil(t, entity4)
	assert.Equal(t, "group4", group4.Name)
	assert.Equal(t, "entity4", entity4.Name)

	group4m, entity5 := store.Next()
	assert.NotNil(t, group4m)
	assert.NotNil(t, entity5)
	assert.Equal(t, group1.Name, group4m.Name)
	assert.Equal(t, entity1.Name, entity5.Name)
}

func TestStore_Next_wraps_around_when_iterating_through_all_entities(t *testing.T) {
	store := setupStore()
	group1, entity1 := store.Next()
	assert.NotNil(t, group1)
	assert.NotNil(t, entity1)
	assert.Equal(t, "group1", group1.Name)
	assert.Equal(t, "entity1", entity1.Name)

	group2, entity2 := store.Next()
	assert.NotNil(t, group2)
	assert.NotNil(t, entity2)
	assert.Equal(t, "group2", group2.Name)
	assert.Equal(t, "entity2", entity2.Name)

	group2m, entity3 := store.Next()
	assert.NotNil(t, group2m)
	assert.NotNil(t, entity3)
	assert.Equal(t, "group2", group2m.Name)
	assert.Equal(t, "entity3", entity3.Name)

	group4, entity4 := store.Next()
	assert.NotNil(t, group4)
	assert.NotNil(t, entity4)
	assert.Equal(t, "group4", group4.Name)
	assert.Equal(t, "entity4", entity4.Name)

	group4m, entity5 := store.Next()
	assert.NotNil(t, group4m)
	assert.NotNil(t, entity5)
	assert.Equal(t, entity1.Name, entity5.Name)
}

func TestStore_Update_updating_a_non_existing_group(t *testing.T) {
	store := setupStore()
	newGroup := placement.NewGroup("group0")
	assert.Nil(t, store.Find("group0"))
	store.Update(time.Now(), newGroup)
	assert.NotNil(t, store.Find("group0"))
}

func TestStore_Update_updating_an_existing_group(t *testing.T) {
	store := setupStore()
	updatedGroup2 := placement.NewGroup("group2")
	store.Update(time.Now(), updatedGroup2)

	assert.Equal(t, 1, len(updatedGroup2.Entities))
	_, exists := updatedGroup2.Entities["entity2"]
	assert.True(t, exists)
	pattern := labels.NewLabel("instance", "A")
	assert.True(t, updatedGroup2.Relations.Contains(pattern))
}

func TestStore_Prune_removes_expired_groups(t *testing.T) {
	store := setupStore().(*store)
	group2Meta := store.groups["group2"]
	group2Meta.creation = group2Meta.creation.Add(-2 * time.Hour)

	assert.Equal(t, 4, len(store.All()))
	store.Prune(time.Now(), time.Hour, time.Duration(0))
	assert.Equal(t, 3, len(store.All()))
}

func TestStore_Prune_keeps_non_expired_groups(t *testing.T) {
	store := setupStore()

	assert.Equal(t, 4, len(store.All()))
	store.Prune(time.Now(), time.Hour, time.Duration(0))
	assert.Equal(t, 4, len(store.All()))
}

func TestStore_Prune_removes_expired_reservations(t *testing.T) {
	store := setupStore()
	assert.Equal(t, 2, len(store.Reserved()))
	store.Prune(time.Now(), time.Hour, time.Duration(0))
	assert.Equal(t, 0, len(store.Reserved()))
}

func TestStore_Prune_keeps_non_expired_reservations(t *testing.T) {
	store := setupStore()
	assert.Equal(t, 2, len(store.Reserved()))
	store.Prune(time.Now(), time.Hour, time.Hour)
	assert.Equal(t, 2, len(store.Reserved()))
}
