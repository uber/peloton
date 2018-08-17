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
}
