import logging
import os
import pytest

from docker import Client
from tools.pcluster.pcluster import setup, teardown


log = logging.getLogger(__name__)


#
# Global setup / teardown across all test suites
#
@pytest.fixture(scope="session", autouse=True)
def setup_cluster(request):
    log.info('setup cluster')
    if os.getenv('CLUSTER', ''):
        log.info('cluster mode')
    else:
        log.info('local pcluster mode')
        setup(enable_peloton=True)

    def teardown_cluster():
        log.info('teardown cluster')
        if os.getenv('CLUSTER', ''):
            log.info('cluster mode, no teardown actions')
        else:
            if os.getenv('NO_TEARDOWN', ''):
                log.info('skip teardown')
                return
            teardown()

    request.addfinalizer(teardown_cluster)


class Container(object):
    def __init__(self, name):
        self._cli = Client(base_url='unix://var/run/docker.sock')
        self._name = name

    def start(self):
        self._cli.start(self.name)
        log.info('%s started', self._name)

    def stop(self):
        self._cli.stop(self.name, timeout=0)
        log.info('%s stopped', self._name)


@pytest.fixture()
def mesos_master():
    return Container('peloton-mesos-master')
