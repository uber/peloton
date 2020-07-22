/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mesosutil

import (
	"testing"
)

func TestNewMainInfo(t *testing.T) {
	main := NewMainInfo("main-1", 1234, 5678)
	if main == nil {
		t.Fatal("Not creating protobuf object MainInfo")
	}
	if main.GetId() != "main-1" {
		t.Fatal("Protobuf object MainInfo.Id missing.")
	}
	if main.GetIp() != 1234 {
		t.Fatal("Protobuf object MainInfo.Ip missing.")
	}
	if main.GetPort() != 5678 {
		t.Fatal("Protobuf object MainInfo.Port missing.")
	}
}
