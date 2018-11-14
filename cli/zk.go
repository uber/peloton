package cli

import (
	"encoding/json"
	"fmt"
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
	json.Unmarshal([]byte(getZKJSONString()), &clustersInfo)

	for i := range clustersInfo.Clusters {
		if clustersInfo.Clusters[i].ClusterName == clusterName {
			return clustersInfo.Clusters[i].ZkURL, nil
		}
	}
	return "", fmt.Errorf("cannot find the corresponding zkURL for %s", clusterName)
}

func getZKJSONString() string {
	zkJSONString := `{
"clusters": [
{
"clusterName": "sjc1-prod02",
"zkURL": "zookeeper-mesos-prod02-sjc1.uber.internal:2181"
},
{
"clusterName": "dca1-preprod01",
"zkURL": "zookeeper-mesos-preprod01-dca1.uber.internal:2181"
},
{
"clusterName": "dca1-prod02",
"zkURL": "zookeeper-mesos-prod02-dca1.uber.internal:2181"
},
{
"clusterName": "dca7-prod02",
"zkURL": "mesoszk-prod02node01-dca7.prod.uber.internal:2181,mesoszk-prod02node02-dca7.prod.uber.internal:2181,mesoszk-prod02node03-dca7.prod.uber.internal:2181,mesoszk-prod02node04-dca7.prod.uber.internal:2181,mesoszk-prod02node05-dca7.prod.uber.internal:2181"
},
{
"clusterName": "pit1-prod01",
"zkURL": "zookeeper-mesos-prod01-pit1.pit-irn-1.uberatc.net:2181"
},
{
"clusterName": "pit1-preprod01",
"zkUrl": "zookeeper-mesos-preprod01.pit-irn-1.uberatc.net:2181"
},
{
"clusterName": "pit1-prod02",
"zkURL": "zookeeper-mesos-prod02-pit1.pit-irn-1.uberatc.net:2181"
},
{
"clusterName": "wbu2-da1",
"zkURL": "10.191.32.38:2181,10.191.32.39:2181,10.191.32.40:2181"
},
{
"clusterName": "wbu2-da2",
"zkURL": "10.126.149.100:2181,10.126.153.44:2181,10.126.144.214:2181,10.126.146.230:2181,10.126.148.70:2181"
},
{
"clusterName": "wbu2-da3w",
"zkURL": "10.191.240.122:2181,10.191.240.140:2181,10.191.243.50:2181,10.191.243.68:2181,10.191.243.74:2181"
},
{
"clusterName": "wbu2-da3e",
"zkURL": "10.191.235.72:2181,10.191.235.96:2181,10.191.235.120:2181,10.191.235.144:2181,10.191.235.168:2181"
},
{
"clusterName": "wbu2-opusdev",
"zkURL": "10.191.235.183:2181,10.191.235.184:2181,10.191.235.185:2181"
}
]
}`

	return zkJSONString
}
