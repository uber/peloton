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

/*
Package model contains all the data container types needed to model your physical and logical infrastructure through
the concept of a group and all your tasks, processes or containers through the concept of an entity. An entity can have
metric requirements - the entity needs so much cpu, memory, disk, etc. - and affinity requirements - the entity
does/does not want to be placed on a group which contains relations from some other entities or the entity does/does not
want to be placed on a group with some certain labels - e.g. a certain sku, having a specific kernel version, etc.
Lastly an entity can also have some soft requirements captured in a strict total ordering to break ties if there are
multiple groups that satisfy the entity's hard requirements.

This package contains the following subpackages that covers the following areas from above:
 - labels contains types used to model labels of a group and relations of an entity for use in the affinity
   requirements.
 - metrics contains types to model sets of metric values.
 - orderings contains types to model orderings to capture the soft requirements of an entity.
 - placements contains the basic group and entity types.
 - requirements contain the types to model metric and affinity requirements.
*/
package model
