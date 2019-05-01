import os
import unittest

from peloton_client.pbgen.mesos.v1 import mesos_pb2 as mesos
from peloton_client.pbgen.peloton.api.v0.task import task_pb2 as task
from peloton_client.pbgen.peloton.api.v0.respool import respool_pb2 as respool
from peloton_client.pbgen.peloton.api.v0 import peloton_pb2 as peloton
from tools.vcluster.config_generator import (
    load_config,
    create_mesos_task_config,
    create_pool_config,
)


class ConfigGeneratorTest(unittest.TestCase):
    def test_create_mesos_task_config(self):
        dynamic_env_master = {"APP": "hostmgr"}
        config_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "config",
            "default.yaml",
        )
        config = load_config(config_file)
        got = create_mesos_task_config(
            config,
            module="peloton",
            dynamic_env=dynamic_env_master,
            image_path="myregistry/peloton:0.1.0",
        )

        expected = task.TaskConfig(
            resource=task.ResourceConfig(
                cpuLimit=2.0, memLimitMb=4096, diskLimitMb=2048
            ),
            ports=[
                task.PortConfig(name="HTTP_PORT", envName="HTTP_PORT"),
                task.PortConfig(name="GRPC_PORT", envName="GRPC_PORT"),
            ],
            container=mesos.ContainerInfo(
                type="MESOS",
                mesos=mesos.ContainerInfo.MesosInfo(
                    image=mesos.Image(
                        type="DOCKER",
                        docker=mesos.Image.Docker(
                            name="myregistry/peloton:0.1.0"
                        ),
                    )
                ),
            ),
            command=mesos.CommandInfo(
                uris=[
                    mesos.CommandInfo.URI(
                        value="https://gist.githubusercontent.com/scy0208/"
                        "08a66afe3a7837e5e1c1528d16b47e6f/raw/"
                        "2119f0fe20b7a1e827e4e43b288545799d6b4e5e/"
                        "hostmgr_mesos_secret",
                        executable=False,
                        cache=False,
                        output_file="hostmgr_mesos_secret",
                    )
                ],
                shell=True,
                value="bash /bin/entrypoint.sh",
                environment=mesos.Environment(
                    variables=[
                        mesos.Environment.Variable(
                            name="CONFIG_DIR", value="config"
                        ),
                        mesos.Environment.Variable(
                            name="AUTO_MIGRATE", value="true"
                        ),
                        mesos.Environment.Variable(
                            name="MESOS_SECRET_FILE",
                            value="/mnt/mesos/sandbox/hostmgr_mesos_secret",
                        ),
                        mesos.Environment.Variable(
                            name="APP", value="hostmgr"
                        ),
                    ]
                ),
            ),
        )

        self.assertEqual(got, expected)

    def test_create_pool_config(self):
        expected = respool.ResourcePoolConfig(
            name="test_respool",
            resources=[
                respool.ResourceConfig(
                    kind="cpu", reservation=1.0, limit=1.0, share=1
                ),
                respool.ResourceConfig(
                    kind="memory", reservation=1024, limit=1024, share=1
                ),
                respool.ResourceConfig(
                    kind="disk", reservation=1024, limit=1024, share=1
                ),
            ],
            parent=peloton.ResourcePoolID(value="root"),
        )

        actual = create_pool_config(
            name="test_respool", cpu=1.0, memory=1024, disk=1024
        )
        self.assertEqual(actual, expected)
