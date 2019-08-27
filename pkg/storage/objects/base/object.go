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

package base

import (
	"reflect"
)

// Definition stores schema information about an Object
type Definition struct {
	// normalized object name
	Name string
	// Primary key of the object
	Key *PrimaryKey
	// Column name to data type mapping of the object
	ColumnToType map[string]reflect.Type
}

// Column holds a column name and value for one row.
type Column struct {
	// Name of the column
	Name string
	// Value of the column
	Value interface{}
}

// ClusteringKey stores name and ordering of a clustering key
type ClusteringKey struct {
	// Name of the clustering key
	Name string
	// Clustering order
	Descending bool
}

// PrimaryKey stores information about partition keys and clustering keys
type PrimaryKey struct {
	// List of partition key names
	PartitionKeys []string
	// List of clustering key objects (clustering key name and order)
	ClusteringKeys []*ClusteringKey
}

// GetColumnsToRead returns a list of column names to be read for this object
// in a select operation
func (o *Definition) GetColumnsToRead() []string {
	colNamesToRead := []string{}
	for col := range o.ColumnToType {
		colNamesToRead = append(colNamesToRead, col)
	}
	return colNamesToRead
}

// Object is a marker interface method that is used to add connector specific
// annotations to storage objects. Users can embed this interface in any
// storage object structure definition.
//
// For example:
// ValidObject is a representation of the orm annotations
// 	type ValidObject struct {
//		base.Object `cassandra:"name=valid_object, primaryKey=((id), name)"`
//		ID          uint64 `column:"name=id"`
//		Name        string `column:"name=name"`
//		Data        string `column:"name=data"`
//	}
// Here, base.Object is embedded in a ValidObject just to specify cassandra
// specific annotations that describe the primary key information as well as
// table name of that object. The partition key is `id` and clustering key is
// `name` while table name is `valid_object`.
//
// The `cassandra` keyword denotes that this annotation is for Cassandra
// connector. The only primary key format supported right now is:
// ((PK1,PK2..), CK1, CK2..)
type Object interface {
	// transform will convert all the value from DB into the corresponding type
	// in ORM object to be interpreted by base store client
	transform(map[string]interface{})
}
