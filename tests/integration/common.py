from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool

import logging
import time
from google.protobuf import json_format
from util import load_test_config

log = logging.getLogger(__name__)


# IntegrationTestConfig is the default integration test config
class IntegrationTestConfig(object):
    def __init__(
        self,
        pool_file="test_respool.yaml",
        max_retry_attempts=60,
        sleep_time_sec=1,
        rpc_timeout_sec=10,
    ):
        respool_config_dump = load_test_config(pool_file)
        respool_config = respool.ResourcePoolConfig()
        json_format.ParseDict(respool_config_dump, respool_config)
        self.respool_config = respool_config

        self.max_retry_attempts = max_retry_attempts
        self.sleep_time_sec = sleep_time_sec
        self.rpc_timeout_sec = rpc_timeout_sec


def wait_for_condition(message, condition, config):
    """
    Waits for a particular condition to be met.
    :param message: log specification, e.g. job id.
    :param condition: the condition to meet.
    :param config: integration test config.
    """
    assert isinstance(config, IntegrationTestConfig)

    attempts, result, start_time = 0, False, time.time()
    log.info("%s waiting for condition %s", message, condition.__name__)
    while attempts < config.max_retry_attempts:
        try:
            result = condition()
            if result:
                break
        except Exception as e:
            log.warn(e)
        time.sleep(config.sleep_time_sec)
        attempts += 1

    if attempts == config.max_retry_attempts:
        log.info("max attempts reached to wait for condition")
        log.info("condition: %s", condition.__name__)
        assert False

    elapsed_time = time.time() - start_time
    log.info(
        "%s waited on condition %s for %s seconds",
        message,
        condition.__name__,
        elapsed_time,
    )
    assert result, "Given condition %s isn't met for %s" % (
        condition.__name__,
        message,
    )
