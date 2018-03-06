import time
import subprocess
import os
from retry import retry
import json

from config_generator import config
from peloton_helper import (
    PelotonClientHelper,
    create_respool_for_new_peloton,
)
from color_print import (
    print_okblue,
    print_okgreen,
)

from modules import (
    Zookeeper,
    MesosMaster,
    MesosSlave,
    Peloton,
)


@retry(tries=3, delay=10)
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
            raise Exception("Cassandra DB migration failed")
    else:
        cmd = config.get('cassandra').get('cmd').get('drop') % keyspace
        print_okgreen(
            "Step: drop Cassandra keyspace %s on host %s" % (
                keyspace, host
            )
        )
        if subprocess.call([cli, host, "-e", cmd, version]) != 0:
            raise Exception("Cassandra DB drop failed")


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

        self.virtual_zookeeper = ''

        # vcluster is the config can be loaded for launching benchmark test
        # it can be dump into the file '.vcluster'
        self.vcluster_config = {}

    def output_vcluster_data(self):
        # write the vcluster data into a json file
        with open('.vcluster', 'w') as outfile:
            json.dump(self.vcluster_config, outfile)

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
        zookeeper_count = int(config.get('zookeeper').get('instance_count'))
        self.zookeeper.setup({}, zookeeper_count)

        # Get zookeeper tasks info
        host, port = self.zookeeper.get_host_port()
        if not host and port:
            raise Exception("Zookeeper launch failed")

        self.virtual_zookeeper = '%s:%s' % (host, port)

        zk_address = 'zk://%s/mesos' % self.virtual_zookeeper
        print_okgreen('Zookeeper created successfully: %s' % zk_address)

        # create mesos master
        self.start_mesos_master(self.virtual_zookeeper)

        # create mesos slaves
        self.start_mesos_slave(self.virtual_zookeeper, agent_num)

        self.vcluster_config.update({
            "Zookeeper": '%s:%s' % (host, port),
            "Mesos Slave Number": agent_num,
        })
        return host, port

    def start_peloton(self, virtual_zookeeper, agent_num, version=None):
        """
        type zk_host: str
        type zk_port: str
        """
        # DB Migration
        cassandra_operation(keyspace=self.label_name, create=True)
        host = config.get('cassandra').get('host')
        time.sleep(10)
        print_okblue("DB migration finished")

        # Setup Peloton
        print_okgreen('Step: Create Peloton, version: %s' % version)

        for app in self.APP_ORDER:
            print_okblue('Creating peloton application: %s' % app)
            dynamic_env_master = {
                'APP': app,
                'ELECTION_ZK_SERVERS': virtual_zookeeper,
                'MESOS_ZK_PATH': 'zk://%s/mesos' % virtual_zookeeper,
                'CASSANDRA_STORE': self.label_name,
                'CASSANDRA_HOSTS': host
            }
            peloton_app_count = int(
                config.get('peloton').get(app).get('instance_count'))
            self.peloton.setup(
                dynamic_env_master, peloton_app_count,
                self.label_name + '_' + 'peloton-' + app,
                version
            )

        self.vcluster_config.update({
            'Peloton Version': version,
        })

        # create a default resource pool
        create_respool_for_new_peloton(
            zk_server=virtual_zookeeper,
            agent_num=agent_num,
        )

    def start_all(self, agent_num, peloton_version):
        """
        type agent_num: int
        """
        host, port = self.start_mesos(agent_num)
        virtual_zookeeper = '%s:%s' % (host, port)
        self.start_peloton(virtual_zookeeper, agent_num, peloton_version)
        self.output_vcluster_data()

    def start_mesos_master(self, virtual_zookeeper):
        zk_address = 'zk://%s/mesos' % virtual_zookeeper
        print_okgreen('Step: creating virtual Mesos-master with 3 instance')
        dynamic_env_master = {
            config.get('mesos-master').get('dynamic_env'): zk_address,
        }
        mesos_count = int(config.get('mesos-master').get('instance_count'))
        self.mesos_master.setup(dynamic_env_master, mesos_count)
        print_okgreen('Mesos-master created successfully.')

    def start_mesos_slave(self, virtual_zookeeper, agent_num):
        # create mesos slaves
        zk_address = 'zk://%s/mesos' % virtual_zookeeper
        print_okgreen(
            'Step: creating virtual Mesos-slave with %s instance' % agent_num
        )
        dynamic_env_slave = {
            config.get('mesos-slave').get('dynamic_env'): zk_address,
        }
        self.mesos_slave.setup(dynamic_env_slave, agent_num)
        print_okgreen('Mesos-slave created successfully.')

    def teardown_slave(self):
        self.mesos_slave.teardown()

    def teardown_peloton(self):
        print_okgreen('Step: stopping all peloton applications')
        for app in reversed(self.APP_ORDER):
            print_okblue('Stopping peloton application: %s' % app)
            self.peloton.teardown(self.label_name + '_' + 'peloton-' + app)

        print_okgreen('Step: drop the cassandra keyspace')
        cassandra_operation(keyspace=self.label_name, create=False)

        try:
            os.remove('.vcluster')
        except OSError:
            pass

    def teardown(self):
        self.teardown_peloton()

        print_okgreen('Step: stopping all virtual Mesos-slaves')
        self.teardown_slave()

        print_okgreen('Step: stopping all virtual Mesos-master')
        self.mesos_master.teardown()

        print_okgreen('Step: stopping all virtual Zookeeper')
        self.zookeeper.teardown()

    def get_vitual_zookeeper(self):
        if self.virtual_zookeeper:
            return self.virtual_zookeeper
        host, port = self.zookeeper.get_host_port()

        return '%s:%s' % (host, port)

    def get_mesos_master(self):
        zk_server = self.get_vitual_zookeeper()
        host, port = self.mesos_master.find_leader(zk_server)
        return '%s:%s' % (host, port)
