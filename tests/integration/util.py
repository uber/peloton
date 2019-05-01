import os
import yaml


test_config_dir = "/test_configs"


#
# Load test config from yaml
#
def load_test_config(config):
    return load_config(config, test_config_dir)


def load_config(config, dir=""):
    config_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)) + dir, config
    )
    with open(config_file, "r") as f:
        config = yaml.load(f)
    return config
