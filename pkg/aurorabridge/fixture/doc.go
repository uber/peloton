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

// ----------------------------------------------------------------------------

// Package fixture provides testing fixtures for aurorabridge.
//
// A fixture is a function which returns a type populated with randomized
// testing data. They are an answer to global test constants and copy-and-pasting
// arbitrarily populated structs around test files. Fixtures are useful for
// high-level tests which just need some valid data to plumb through mocks.
//
// The philosophy of fixtures is that tests should only declare data relevant
// to the test. Tests should be easy to write, easier to read, and focus only
// on details that affect the passage or failure of the test. Anything else is
// a distraction.
//
// When to use a fixture:
// - Your test doesn't care what the data is, it just needs something to mock
//   against.
// - Your test needs a lot of unique data.
//
// When to NOT use a fixture:
// - Your test depends on some carefully constructed data, e.g. events ordered
//   in a special way or integers with an exact value. In these cases, construct
//   the data directly in the test.
//
// When to maybe use a fixture:
// - Your test doesn't care what *most* of the data is, but is very interested
//   in a specific field. In this case, it can be appropriate to call a fixture
//   but overwrite the desired field.
//
// What fixtures should look like:
// - Prefers building on other fixtures instead of taking parameters. Note, in
//   some situations parameters can still be useful as long as they are simple.
// - Random values still make sense semantically, e.g. instance count is between
//   1-10000 and service name looks like "service-ahgb73".
package fixture
