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

package placement

import (
	"sync"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
)

// NewScopeSet creates a new scope set for use in computations that need the label or relation scope of a group.
func NewScopeSet(scopeGroups []*Group) *ScopeSet {
	return &ScopeSet{
		scopeGroups: scopeGroups,
		cache:       map[string]*scopeResult{},
	}
}

// ScopeSet contains a set of pre-computed label or relation bags for a given group and scope. The methods label scope
// and relation scope will return the labels or relations in the scope if they are pre-computed, else they will be
// computed and stored for the next call.
type ScopeSet struct {
	scopeGroups []*Group
	cache       map[string]*scopeResult
	lock        sync.Mutex
}

type scopeResult struct {
	labels    *labels.Bag
	relations *labels.Bag
	groups    []*Group
}

func (set *ScopeSet) scope(group *Group, scope *labels.Label) *scopeResult {
	if scope == nil {
		return &scopeResult{
			groups:    []*Group{group},
			labels:    group.Labels,
			relations: group.Relations,
		}
	}
	groupLabels := group.Labels.Find(scope)
	for _, label := range groupLabels {
		if result, exists := set.cache[label.String()]; exists {
			return result
		}
	}

	var groupsResult []*Group
	labelsResult := labels.NewBag()
	relationsResult := labels.NewBag()
	for _, scopeGroup := range set.scopeGroups {
		for _, scopeLabel := range groupLabels {
			if scopeGroup.Labels.Contains(scopeLabel) {
				groupsResult = append(groupsResult, scopeGroup)
				labelsResult.AddAll(scopeGroup.Labels)
				relationsResult.AddAll(scopeGroup.Relations)
				break
			}
		}
	}

	result := &scopeResult{
		groups:    groupsResult,
		labels:    labelsResult,
		relations: relationsResult,
	}
	for _, label := range groupLabels {
		set.cache[label.String()] = result
	}

	return result
}

// ScopeGroups returns the set of groups, given to a scope set when it is created, which are used as the basis for all
// scope calculations.
func (set *ScopeSet) ScopeGroups() []*Group {
	return append([]*Group{}, set.scopeGroups...)
}

// CompleteScope returns the union of all groups that have ever been in scope for any call to LabelScope or
// RelationScope. Notice that the result will change depending on which calls are made to LabelScope and RelationScope.
func (set *ScopeSet) CompleteScope() map[string]*Group {
	set.lock.Lock()
	defer set.lock.Unlock()

	groups := map[string]*Group{}
	for _, result := range set.cache {
		for _, group := range result.groups {
			groups[group.Name] = group
		}
	}

	return groups
}

// LabelScope finds all labels in scope of the given group and caches them for the next call.
func (set *ScopeSet) LabelScope(group *Group, scope *labels.Label) *labels.Bag {
	set.lock.Lock()
	defer set.lock.Unlock()

	result := set.scope(group, scope)
	return result.labels
}

// RelationScope finds all relations in scope of the given group and caches them for the next call.
func (set *ScopeSet) RelationScope(group *Group, scope *labels.Label) *labels.Bag {
	set.lock.Lock()
	defer set.lock.Unlock()

	result := set.scope(group, scope)
	return result.relations
}

// Copy makes a shallow copy of the scope set where the label bags are the same as in the original.
func (set *ScopeSet) Copy() *ScopeSet {
	set.lock.Lock()
	defer set.lock.Unlock()

	result := NewScopeSet(set.scopeGroups)
	for key, value := range set.cache {
		result.cache[key] = value
	}
	return result
}
