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

package orm

import (
	"regexp"
	"strings"

	"github.com/uber/peloton/pkg/storage/objects/base"

	"go.uber.org/yarpc/yarpcerrors"
)

const (
	// connectorTag will describe the connector specific annotations on any
	// storage object
	connectorTag = "cassandra"
	// columnTag will describe column specific annotations on every storage
	// object field
	columnTag = "column"
	// "Object" is the reflection name of the marker interface used to embed DB
	// annotations in storage objects
	objectName = "Object"
)

var (
	primaryKeyTagPattern = regexp.MustCompile(
		`primaryKey\s*=\s*([^=]*)((\s+.*=)|$)`)
	// primaryKeyPattern is regex for the format((PK1,PK2..), CK1, CK2..)
	primaryKeyPattern = regexp.MustCompile(`\(\s*\((.*)\)(.*)\)`)
	namePattern       = regexp.MustCompile(`name\s*=\s*(\S*)`)
)

// parseClusteringKeys func parses the clustering key of storage object
// The clustering key string should be a comma separated string of individual
// clustering keys. Example "CK1, CK2, CK3 ..."
func parseClusteringKeys(ckStr string) ([]*base.ClusteringKey, error) {
	ckStr = strings.TrimSpace(ckStr)
	cks := strings.Split(ckStr, ",")
	var clusteringKeys []*base.ClusteringKey
	for _, ck := range cks {
		fields := strings.Fields(ck)
		// in case CK format is wrong, don't crash
		if len(fields) == 0 {
			continue
		}
		clusteringKeys = append(
			clusteringKeys, &base.ClusteringKey{
				Name:       strings.TrimSpace(fields[0]),
				Descending: true,
			})
	}
	return clusteringKeys, nil
}

// parsePartitionKey func parses the partition key of storage object
// The partition key string should be a comma separated string of individual
// partition keys. Example "PK1, PK2, PK3 ..."
func parsePartitionKey(pkStr string) []string {
	pkStr = strings.TrimSpace(pkStr)
	var pks []string
	partitionKeys := strings.Split(pkStr, ",")
	for _, pk := range partitionKeys {
		npk := strings.TrimSpace(pk)
		if len(pk) > 0 {
			pks = append(pks, npk)
		}
	}
	return pks
}

// parsePrimaryKey func parses the primary key of storage object
// Primary key string should be of the format((PK1,PK2..), CK1, CK2..)
func parsePrimaryKey(pkStr string) (*base.PrimaryKey, error) {
	var partitionKeyStr string
	var clusteringKeyStr string
	// filter out "trailing comma and space"
	pkStr = strings.TrimRight(pkStr, ", ")
	pkStr = strings.TrimSpace(pkStr)
	matched := false

	// primaryKey=((PK1,PK2), CK1, CK2)
	matchs := primaryKeyPattern.FindStringSubmatch(pkStr)
	if len(matchs) == 3 {
		matched = true
		partitionKeyStr = matchs[1]
		clusteringKeyStr = matchs[2]
	}

	if !matched {
		return nil, yarpcerrors.InternalErrorf(
			"invalid primary key: %s", pkStr)
	}
	partitionKeys := parsePartitionKey(partitionKeyStr)
	clusteringKeys, err := parseClusteringKeys(clusteringKeyStr)
	if err != nil {
		return nil, err
	}

	return &base.PrimaryKey{
		PartitionKeys:  partitionKeys,
		ClusteringKeys: clusteringKeys,
	}, nil
}

// parseNameTag functions parses object "name" tag to get the field name
func parseNameTag(tag string) (string, error) {
	name := ""
	matches := namePattern.FindStringSubmatch(tag)
	if len(matches) == 2 {
		name = matches[1]
	}
	name = strings.TrimRight(name, " ,")
	if name == "" {
		return "", yarpcerrors.InternalErrorf(
			"couldn't derive name from tag %v", tag)
	}
	return name, nil
}

// parseCassandraObjectTag function parses Cassandra specifc ORM annotation on
// the "Object" field of the storage object
func parseCassandraObjectTag(ormAnnotation string) (
	string, *base.PrimaryKey, error) {
	tag := ormAnnotation

	// find the primaryKey
	matchs := primaryKeyTagPattern.FindStringSubmatch(tag)
	if len(matchs) != primaryKeyTagPattern.NumSubexp()+1 {
		return "", nil, yarpcerrors.InternalErrorf(
			"primary key pattern mismatch for tag %v", tag)
	}
	pkString := matchs[1]

	key, err := parsePrimaryKey(pkString)
	if err != nil {
		return "", nil, err
	}

	// Remove the partition and clustering key strings from the base tag
	// string so that we can use the new tag to extract the name
	toRemove := strings.TrimSuffix(matchs[0], matchs[2])
	toRemove = strings.TrimSuffix(matchs[0], matchs[3])
	tag = strings.Replace(tag, toRemove, "", 1)
	name, err := parseNameTag(tag)
	if err != nil {
		return "", nil, err
	}

	return name, key, nil
}
