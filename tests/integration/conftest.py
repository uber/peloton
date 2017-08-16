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
    def __init__(self, names):
        self._cli = Client(base_url='unix://var/run/docker.sock')
        self._names = names

    def start(self):
        for name in self._names:
            self._cli.start(name)
            log.info('%s started', name)

    def stop(self):
        for name in self._names:
            self._cli.stop(name, timeout=0)
            log.info('%s stopped', name)


@pytest.fixture()
def mesos_master():
    return Container(['peloton-mesos-master'])


@pytest.fixture()
def jobmgr():
    # TODO: We need to pick up the count dynamically.
    return Container(['peloton-jobmgr0', 'peloton-jobmgr1'])
