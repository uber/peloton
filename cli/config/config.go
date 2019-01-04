package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"

	"github.com/pkg/errors"
)

const (
	// config is a configuration JSON file that specifies available clusters,
	// users can create it themselves and populate it with clusters' name and zk
	// info or clone it from a peloton cluster config repo.
	// Format as
	// {
	//  "clusters":[
	//    {
	//      "clusterName":"xxx",
	//      "zkURL":"xxxx"
	//    }]
	// }// If clusters.json does not exist in either place, return err
	// the location of this file is either ~/.peloton or /etc/peloton
	configName = "clusters.json"
	// Cli will read from a well defined set of paths on disk in this order
	// 1) ~/.peloton"
	// 2) /etc/peloton
	configPathUserDir   = "/.peloton/"
	configPathSystemDir = "/etc/peloton/"
)

// ReadZKConfigFile read the clusters info config file
func ReadZKConfigFile() ([]byte, error) {
	user, err := user.Current()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("no user found"))
	}
	configPathUser := filepath.Join(user.HomeDir, configPathUserDir, configName)
	configPathSystem := filepath.Join(configPathSystemDir, configName)
	configPaths := [2]string{configPathUser, configPathSystem}
	// Check clusters.json in ~/.peloton/, if not exist, check /etc/pelton
	for _, path := range configPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		}
		file, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("unable to "+
				"open file %s", path))
		}
		return file, nil
	}
	return nil, fmt.Errorf("unable to "+
		"find file %s or %s, "+
		"please create this file or clone it from a cluster config"+
		" repo",
		configPathUser, configPathSystem)
}

// ClustersInfoType is the struct containing the zk information of all the
// clusters
type ClustersInfoType struct {
	Clusters []ClusterInfoType
}

// ClusterInfoType is the struct containing the zk information of one cluster
type ClusterInfoType struct {
	ClusterName string
	ZkURL       string
}

// GetZkInfoFromClusterName returns the zk information of this cluster provided
func GetZkInfoFromClusterName(clusterName string,
	zkJSONBytes []byte) (zkURL string, err error) {
	var clustersInfo ClustersInfoType
	if e := json.Unmarshal(zkJSONBytes, &clustersInfo); e != nil {
		return "", errors.Wrap(e, "invalid json string")
	}

	for i := range clustersInfo.Clusters {
		if clustersInfo.Clusters[i].ClusterName == clusterName {
			return clustersInfo.Clusters[i].ZkURL, nil
		}
	}
	return "", fmt.Errorf("cannot find the corresponding "+
		"zk url for %s", clusterName)
}
