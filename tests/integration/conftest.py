import os
import pytest

from tools.pcluster.pcluster import setup, teardown


#
# Global setup / teardown across all test suites
#
@pytest.fixture(scope="session", autouse=True)
def setup_cluster(request):
    print 'setup cluster'
    if os.getenv('CLUSTER', ''):
        print 'cluster mode'
    else:
        print 'local pcluster mode'
        setup(enable_peloton=True)

    def teardown_cluster():
        print 'teardown cluster'
        if os.getenv('CLUSTER', ''):
            print 'cluster mode, no teardown actions'
        else:
            if os.getenv('NO_TEARDOWN', ''):
                print 'skip teardown'
                return
            teardown()

    request.addfinalizer(teardown_cluster)
