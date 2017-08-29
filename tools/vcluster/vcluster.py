import time
import subprocess
import os

from task_config import config

from peloton_helper import PelotonClientHelper

from color_print import (
    print_fail,
    print_okblue,
    print_okgreen,
)

from modules import (
    Zookeeper,
    MesosMaster,
    MesosSlave,
    Peloton
)


def cassandra_operation(keyspace, create=True):
    """
    rtype: bool
    """
    cli = config.get('cassandra').get('cli')
    host = config.get('cassandra').get('host')
    migrate_cli = config.get('cassandra').get('migrate_cli') % os.environ[
        'GOPATH']
    migrate_cmd = config.get('cassandra').get('cmd').get('migrate') % (
                    host, keyspace)
    path = config.get('cassandra').get('path')

    version = config.get('cassandra').get('version')
    if create:
        # Create DB
        create_script = config.get('cassandra').get('cmd').get(
            'create') % keyspace
        cmd_create = [cli, host, "-e", create_script, version]
        print_okgreen(
            "Step: creating Cassandra keyspace %s on host %s" % (
                keyspace, host
            )
        )
        return_create = subprocess.call(cmd_create)
        time.sleep(5)

        # Migrate DB
        cmd_migrate = [migrate_cli,
                       '--url', migrate_cmd,
                       '--path', path,
                       'up']
        print_okgreen(
            "Step: migrating Cassandra %s from %s" % (
                migrate_cmd, path
            )
        )

        return_migrate = subprocess.call(cmd_migrate)
        if return_create != 0 | return_migrate != 0:
            return False
    else:
        cmd = config.get('cassandra').get('cmd').get('drop') % keyspace
        print_okgreen(
            "Step: drop Cassandra keyspace %s on host %s" % (
                keyspace, host
            )
        )
        return subprocess.call([cli, host, "-e", cmd, version]) == 0
    return True


class VCluster(object):

    APP_ORDER = ['hostmgr', 'resmgr', 'placement', 'jobmgr']

    def __init__(self, label_name, zk_server, respool_path):
        """
        param label_name: label of the virtual cluster
        param zk_server: DNS address of the physical zookeeper server
        param respool_path: the path of the resource pool

        type label_name: str
        type zk_server: str
        type respool: str
        """
        self.label_name = label_name
        self.zk_server = zk_server
        self.respool_path = respool_path

        self.peloton_helper = PelotonClientHelper(zk_server, respool_path)

        self.zookeeper = Zookeeper(self.label_name, self.peloton_helper)
        self.mesos_master = MesosMaster(self.label_name, self.peloton_helper)
        self.mesos_slave = MesosSlave(self.label_name, self.peloton_helper)

        # Optionally includes Peloton apps
        self.peloton = Peloton(self.label_name, self.peloton_helper)

    def start_mesos(self, agent_num):
        """
        param agent_num: Mesos-agent number of the virtual cluster
        type agent_num: int

        return host, port: zookeeper host, zookeeper port
        rtype host: str
        """
        # setup the VCluster on a physical Peloton cluster
        # create zookeeper
        print_okgreen('Step: creating virtual zookeeper with 1 instance')
        self.zookeeper.setup({}, 1)

        # Get zookeeper tasks info
        host, port = self.zookeeper.get_host_port()
        zk_address = 'zk://{0}:{1}/mesos'.format(host, port)
        print_okgreen('Zookeeper created successfully: %s' % zk_address)

        # create mesos master
        print_okgreen('Step: creating virtual Mesos-master with 3 instance')
        dynamic_env_master = {
            config.get('mesos-master').get('dynamic_env'): zk_address,
        }
        self.mesos_master.setup(dynamic_env_master, 3)
        print_okgreen('Mesos-master created successfully.')

        # create mesos master
        print_okgreen(
            'Step: creating virtual Mesos-master with %s instance' % agent_num
        )
        dynamic_env_slave = {
            config.get('mesos-slave').get('dynamic_env'): zk_address,
        }
        self.mesos_slave.setup(dynamic_env_slave, agent_num)
        print_okgreen('Mesos-master created successfully.')

        print_okgreen('Virtual Mesos cluster start successfully.')
        return host, port

    def start_peloton(self, zk_host, zk_port):
        """
        type zk_host: str
        type zk_port: str
        """
        # DB Migration
        if not cassandra_operation(keyspace=self.label_name, create=True):
            print_fail("DB migration failed")
            return
        time.sleep(10)
        print_okblue("DB migration finished")

        # Setup Peloton
        print_okgreen('Step: Create Peloton applications')

        zk = '%s:%s' % (zk_host, zk_port)

        for app in self.APP_ORDER:
            print_okblue('Creating peloton application: %s' % app)
            dynamic_env_master = {
                'APP': app,
                'ELECTION_ZK_SERVERS': zk,
                'MESOS_ZK_PATH': 'zk://%s/mesos' % zk,
                'CASSANDRA_STORE': self.label_name
            }
            self.peloton.setup(
                dynamic_env_master, 2,
                self.label_name + '_' + 'peloton-' + app
            )

    def start_all(self, agent_num):
        """
        type agent_num: int
        """
        host, port = self.start_mesos(agent_num)
        self.start_peloton(host, port)

    def teardown(self):
        print_okgreen('Step: stopping all peloton applications')
        for app in reversed(self.APP_ORDER):
            print_okblue('Stopping peloton application: %s' % app)
            self.peloton.teardown(self.label_name + '_' + 'peloton-' + app)

        print_okgreen('Step: drop the cassandra keyspace')
        cassandra_operation(keyspace=self.label_name, create=False)

        print_okgreen('Step: stopping all virtual Mesos-slaves')
        self.mesos_slave.teardown()

        print_okgreen('Step: stopping all virtual Mesos-master')
        self.mesos_master.teardown()

        print_okgreen('Step: stopping all virtual Zookeeper')
        self.zookeeper.teardown()
