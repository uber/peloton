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
