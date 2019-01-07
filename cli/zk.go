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

package cli

import (
	"encoding/json"
	"fmt"

	"github.com/gobuffalo/packr"
)

const (
	configPath = "../config/zk"
	configName = "zk.json"
)

// ClustersInfoType is the struct containing the zk information of all the clusters
type ClustersInfoType struct {
	Clusters []ClusterInfoType
}

// ClusterInfoType is the struct containing the zk information of one cluster
type ClusterInfoType struct {
	ClusterName string
	ZkURL       string
}

// GetZkInfoFromClusterName returns the zk information of this cluster provided
func GetZkInfoFromClusterName(clusterName string) (zkURL string, err error) {
	var clustersInfo ClustersInfoType
	zkJSONBytes, err := readConfigFile(configPath, configName)
	if err != nil {
		return "", err
	}
	if e := json.Unmarshal(zkJSONBytes, &clustersInfo); e != nil {
		return "", fmt.Errorf("invalid json string %s", configName)
	}

	for i := range clustersInfo.Clusters {
		if clustersInfo.Clusters[i].ClusterName == clusterName {
			return clustersInfo.Clusters[i].ZkURL, nil
		}
	}
	return "", fmt.Errorf("cannot find the corresponding zk url for %s", clusterName)
}

func readConfigFile(filePath string, fileName string) ([]byte, error) {
	box := packr.NewBox(filePath)
	zkJSONBytes, err := box.Find(fileName)
	if err != nil {
		return nil, fmt.Errorf("unable to open file %s", fileName)
	}

	return zkJSONBytes, nil
}
