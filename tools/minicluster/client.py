import json
import logging

from peloton_client.client import PelotonClient

log = logging.getLogger(__name__)


class PelotonClientWrapper(PelotonClient):
    """
    Wrapper on the standard Peloton client to patch certain
    parts of the data retrieved from Zookeeper. This is
    needed to workaround the fact that Docker containers
    created by minicluster cannot be directly addressed with
    the information found in Zookeeper.
    """

    def _on_job_mgr_leader_change(self, data, stat, event):
        data = self._patch_leader_ip("job_mgr", data)
        super(PelotonClientWrapper, self)._on_job_mgr_leader_change(
            data, stat, event
        )

    def _on_res_mgr_leader_change(self, data, stat, event):
        data = self._patch_leader_ip("res_mgr", data)
        super(PelotonClientWrapper, self)._on_res_mgr_leader_change(
            data, stat, event
        )

    def _on_host_mgr_leader_change(self, data, stat, event):
        data = self._patch_leader_ip("host_mgr", data)
        super(PelotonClientWrapper, self)._on_host_mgr_leader_change(
            data, stat, event
        )

    def _patch_leader_ip(self, comp_name, data):
        if data.startswith("{"):
            try:
                leader = json.loads(data)
                leader["ip"] = "localhost"
                log.info("Patching %s leader with %s", comp_name, leader)
                data = json.dumps(leader)
            except Exception as e:
                log.warn("Failed to patch leader data: %s", e)
        return data
