import unittest

from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.task import task_pb2 as task
from tools.vcluster.config_generator import create_mesos_task_config


class ConfigGeneratorTest(unittest.TestCase):

    def test_create_mesos_task_config(self):
        dynamic_env_master = {
            'APP': 'hostmgr',
        }
        got = create_mesos_task_config(
            module='peloton',
            dynamic_env=dynamic_env_master,
            version='1.0.0')

        expected = task.TaskConfig(
            resource=task.ResourceConfig(
                cpuLimit=2.0,
                memLimitMb=2048,
                diskLimitMb=2048,
            ),
            ports=[task.PortConfig(
                    name='HTTP_PORT',
                    envName='HTTP_PORT',
                ), task.PortConfig(
                    name='GRPC_PORT',
                    envName='GRPC_PORT')],
            container=mesos.ContainerInfo(
                type='MESOS',
                mesos=mesos.ContainerInfo.MesosInfo(
                    image=mesos.Image(
                        type='DOCKER',
                        docker=mesos.Image.Docker(
                            name='docker-registry.pit-irn-1.uberatc.net'
                                 '/vendor/peloton:1.0.0',
                        )
                    )
                ),
            ),
            command=mesos.CommandInfo(
                uris=[mesos.CommandInfo.URI(
                    value='https://gist.githubusercontent.com/scy0208/'
                          '08a66afe3a7837e5e1c1528d16b47e6f/raw/'
                          '2119f0fe20b7a1e827e4e43b288545799d6b4e5e/'
                          'hostmgr_mesos_secret',
                    output_file='hostmgr_mesos_secret',
                )],
                shell=True,
                value='bash /bin/entrypoint.sh',
                environment=mesos.Environment(
                    variables=[mesos.Environment.Variable(
                        name='CONFIG_DIR',
                        value='config',
                    ), mesos.Environment.Variable(
                        name='AUTO_MIGRATE',
                        value='false',
                    ), mesos.Environment.Variable(
                        name='MESOS_SECRET_FILE',
                        value='/mnt/mesos/sandbox/hostmgr_mesos_secret',
                    ), mesos.Environment.Variable(
                        name='APP',
                        value='hostmgr',
                    )]
                )
            ),
        )

        self.assertEqual(got, expected)
