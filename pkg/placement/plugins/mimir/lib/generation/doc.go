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
Package generation provides builders to easily build groups, entities and their resource and affinity requirements and
placement orderings. All builders have methods to set each specific field of the type being build and a generate method
which will then create an instance of the type from the specifications given to the builder.

Building a group can look like this:
	random := rand.New(rand.NewSource(42))
	// Create a template for the group name
	nameTemplate := labels.NewLabelTemplate("schemadock$id$-$dc$")
	// Create the builder
	builder := NewGroupBuilder().
		Name(nameTemplate)
	// Bind values to the names in the template, this makes reusing the builder to generate groups with different
	// names easy.
	nameTemplate.
		Bind("id", "42").
		Bind("dc", "dc1")
	// Generate a group with name "schemadock42-dc1"
	group := builder.Generate(random, time.Now())
*/
package generation
