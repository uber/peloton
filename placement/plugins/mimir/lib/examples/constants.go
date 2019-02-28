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

package examples

import "fmt"

const (
	// Instance is used to represent a Schemaless instance in label templates.
	Instance = Variable("instance")

	// Cluster is used to represent a Schemaless cluster in label templates.
	Cluster = Variable("cluster")

	// Database is used to represent a Schemaless database in label templates.
	Database = Variable("database")

	// Datacenter is used to represent a datacenter name in label templates.
	Datacenter = Variable("datacenter")

	// Host is used to represent a host name in label templates.
	Host = Variable("host")

	// Rack is used to represent a rack name in label templates.
	Rack = Variable("rack")

	// VolumeType is used to represent a volume type of a group.
	VolumeType = Variable("volume-type")
)

// Variable represents a variable that can be used in a label template.
type Variable string

// Name returns the name of the variable.
func (variable Variable) Name() string {
	return string(variable)
}

// Variable returns the name of the variable quoted with $-signs.
func (variable Variable) Variable() string {
	return fmt.Sprintf("$%v$", variable)
}
