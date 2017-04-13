package leader

import (
	"net/url"

	"code.uber.internal/infra/peloton/common"

	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
)

// Discovery is the service discovery interface for Peloton CLI
type Discovery interface {
	GetJobMgrURL() (*url.URL, error)
	GetResMgrURL() (*url.URL, error)
}

// NewStaticServiceDiscovery creates a staticDiscovery object
func NewStaticServiceDiscovery(
	jobmgrURL *url.URL,
	resmgrURL *url.URL) (Discovery, error) {

	discovery := &staticDiscovery{
		jobmgrURL: jobmgrURL,
		resmgrURL: resmgrURL,
	}

	discovery.jobmgrURL.Path = common.PelotonEndpointPath
	discovery.resmgrURL.Path = common.PelotonEndpointPath

	return discovery, nil
}

// staticDiscovery is the static implementation of Discovery
type staticDiscovery struct {
	jobmgrURL *url.URL
	resmgrURL *url.URL
}

// GetJobMgrURL returns jobmgrURL
func (s *staticDiscovery) GetJobMgrURL() (*url.URL, error) {
	return s.jobmgrURL, nil
}

// GetResMgrURL returns resmgrURL
func (s *staticDiscovery) GetResMgrURL() (*url.URL, error) {
	return s.resmgrURL, nil
}

// NewZkServiceDiscovery creates a zkDiscovery object
func NewZkServiceDiscovery(
	zkServers []string,
	zkRoot string) (Discovery, error) {

	zkClient, err := zookeeper.New(
		zkServers,
		&store.Config{ConnectionTimeout: zkConnErrRetry},
	)
	if err != nil {
		return nil, err
	}

	discovery := &zkDiscovery{
		zkClient: zkClient,
		zkRoot:   zkRoot,
	}
	return discovery, nil
}

// zkDiscovery is the zk based implementation of Discovery
type zkDiscovery struct {
	zkClient store.Store
	zkRoot   string
}

// GetJobMgrURL reads jobmgr leader url from zk and returns jobmgrURL
func (s *zkDiscovery) GetJobMgrURL() (*url.URL, error) {
	zkPath := leaderZkPath(s.zkRoot, common.JobManagerRole)
	leader, err := s.zkClient.Get(zkPath)
	if err != nil {
		return nil, err
	}

	jobmgrURL, err := parseURL(string(leader.Value))
	if err != nil {
		return nil, err
	}

	return jobmgrURL, nil
}

// GetResMgrURL reads resmgr leader url from zk and returns resmgrURL
func (s *zkDiscovery) GetResMgrURL() (*url.URL, error) {
	zkPath := leaderZkPath(s.zkRoot, common.ResourceManagerRole)
	leader, err := s.zkClient.Get(zkPath)
	if err != nil {
		return nil, err
	}

	resmgrURL, err := parseURL(string(leader.Value))
	if err != nil {
		return nil, err
	}

	return resmgrURL, nil
}

// parseURL generates url.URL from urlStr and sets path
func parseURL(urlStr string) (*url.URL, error) {
	url, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	url.Path = common.PelotonEndpointPath

	return url, nil
}
