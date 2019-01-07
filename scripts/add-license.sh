#!/bin/bash

FILES=$(find . -name "*.go" | grep -v mimir-lib |grep -v mesos-go)

LICENSE=$(cat <<-END
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
END
)

for file in $FILES
do
    # Check if we have license already
    has_license=$(grep -c 'Licensed under the Apache License' $file)
    if [[ $has_license -eq 1 ]]
    then
	continue
    fi

    # Prepend the license to the file
    echo "$LICENSE

$(cat $file)" > $file    

done
