from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool

from google.protobuf import json_format
from util import load_test_config


# IntegrationTestConfig is the default integration test config
class IntegrationTestConfig(object):
    def __init__(self, pool_file='test_respool.yaml', max_retry_attempts=60,
                 sleep_time_sec=1, rpc_timeout_sec=10):
        respool_config_dump = load_test_config(pool_file)
        respool_config = respool.ResourcePoolConfig()
        json_format.ParseDict(respool_config_dump, respool_config)
        self.respool_config = respool_config

        self.max_retry_attempts = max_retry_attempts
        self.sleep_time_sec = sleep_time_sec
        self.rpc_timeout_sec = rpc_timeout_sec
