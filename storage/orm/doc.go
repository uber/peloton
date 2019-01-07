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

/*
Package orm implements the ORM (object relational mapping) layer in
peloton. There are three major components of this layer:

  * Object - is the object representation of a DB table. So every table in DB
             should be read or written using a storage object. Each field of the
             storage object corresponds to each column of the DB table. Storage
             object is annotated using a limited DSL so that ORM can
             transalate the object into UQL queries. Each application that wants
             to store any data to the DB must maintain an up to date storage
             object corresponding to the DB table. For example, jobmgr reads and
             writes secrets from secret_info table, so it will maintain
             an storage object SecretObject all of whose fields will map to the
             schema of secret_info table. The mapping will be described using
             ORM annotations on the SecretObject struct.

  * Client - is the interface exposed by ORM to the application layer.
             Any application (ex: jobmgr) which wants to do storage operations
             must do it using the API exposed by the ORM Client.

  * Connector - is the interface mapping directly to the API exposed by the
             client and should be implemented by different storage connectors.
             Peloton currently has a cassandra implementation of the connector
             and we can extend this to other DBs.
*/
