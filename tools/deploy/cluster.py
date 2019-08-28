from __future__ import absolute_import

import yaml
import json
import json_delta
from prompter import yesno
from thrift import TSerialization
from thrift.protocol.TJSONProtocol import TSimpleJSONProtocolFactory

from aurora.client import AuroraClientZK
from app import App, CronApp

# Currently DB schema migration is executed only in hostmgr so we have
# to update hostmgr first.
# TODO: use a separate binary for DB migration
PELOTON_APPS = [
    "hostmgr",
    "resmgr",
    "placement",
    "placement_stateful",
    "placement_stateless",
    "jobmgr",
    "archiver",
    "aurorabridge",
    "apiserver",
]
CRON_APPS = ["watchdog"]


def update_callback(app):
    """
    Callback function for updating Peloton apps so that we can run
    integration tests in-between updates of each app.
    """
    print("Update callback invoked for %s" % app.name)

    # TODO: Add integration tests here
    return True


class Cluster(object):
    """
    Representation of a Peloton cluster
    """

    def __init__(self, cfg_file, **kwargs):
        self.auto_migrate = False
        self.use_host_pool = False
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

        self.client = AuroraClientZK.create(
            zk_endpoints=self.zookeeper, zk_path=self.aurora_zk_path
        )

        self.cfg_file = cfg_file
        self.apps = []
        for app in PELOTON_APPS:
            app_cfg = getattr(self, app, None)
            if app_cfg is None:
                continue
            self.apps.append(App(name=app, cluster=self, **app_cfg))
        for app in CRON_APPS:
            app_cfg = getattr(self, app, None)
            if app_cfg is not None:
                self.apps.append(CronApp(name=app, cluster=self, **app_cfg))
            else:
                print(
                    "Skipping cron app, existing cron will have to be disabled using `aurora deschedule` command"
                )

    @staticmethod
    def load(cfg_file):
        """
        Load the cluster config from a yaml file
        """
        with open(cfg_file, "r") as f:
            try:
                cfg = yaml.safe_load(f)
            except yaml.YAMLError as ex:
                print("Failed to unmarshal cluster config %s" % cfg_file)
                raise ex

        return Cluster(cfg_file, **cfg)

    def diff_config(self, app, verbose=False):
        """
        Print the diff between current and desired job config
        """
        print(">>>>>>>> Job config diff for %s <<<<<<<<" % app.name)
        cfg_dicts = []
        factory = TSimpleJSONProtocolFactory()
        for cfg in app.current_job_config, app.desired_job_config:
            if cfg:
                cfg_json = TSerialization.serialize(
                    cfg, protocol_factory=factory
                )
                cfg_dict = json.loads(cfg_json)

                # Unset task resources to avoid confusing the job config differ
                cfg_dict["taskConfig"]["resources"] = None
            else:
                cfg_dict = {}
            cfg_dicts.append(cfg_dict)

        if verbose:
            for cfg_dict in cfg_dicts:
                print(json.dumps(cfg_dict, indent=4, sort_keys=True))

        for line in json_delta.udiff(cfg_dicts[0], cfg_dicts[1]):
            print(line)

    def update(self, force, verbose):
        """
        Rolling update the Peloton apps in the cluster
        """

        # Print the job config diffs
        print('Update Peloton cluster "%s" to new config: ' % self.name)
        for app in self.apps:
            self.diff_config(app, verbose)

        if not force and not yesno("Proceed with the update ?"):
            return

        updated_apps = []
        for app in self.apps:
            updated_apps.append(app)
            if not app.update_or_create_job(update_callback):
                # Rollback the updates for all apps that have been updated
                self.rollback(updated_apps)
                return False

        return True

    def rollback(self, apps):
        """
        Rollback the updates to the list of apps in the cluster
        """
        while len(apps) > 0:
            app = apps.pop()
            print("Rolling back app %s ..." % app.name)
            app.rollback_job()
