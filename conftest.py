import json
import logging
import os
import time

import pytest

import dcos_launch.config
import dcos_launch.util
import yaml
from dcos_launch import util
from dcos_test_utils import logger
from rc_support import init_runtime_config

logger.setup('DEBUG')

log = logging.getLogger(__name__)


def _write_yaml(y: dict, path: str) -> None:
    try:
        with open(path, "w") as f:
            return yaml.safe_dump(y, f)
    except yaml.YAMLError as ex:
        raise util.LauncherError('InvalidYaml', None) from ex
    except FileNotFoundError as ex:
        raise util.LauncherError('MissingConfig', None) from ex


def _merge_launch_config_with_env(launch_config_path: str) -> None:
    """ This method will modify a file being used for launch by injecting in
    an environment variable for the installer_url.

    Test options change depending on the installer and most testing for
    upgrades will be done from specific pinned versions, which is the latest
    stable minor release for a given major release
    """
    installer_url = os.getenv("TEST_LAUNCH_CONFIG_INSTALLER_URL")
    if installer_url:
        log.info('TEST_LAUNCH_CONFIG_INSTALLER_URL found: {}'.format(installer_url))
        initial_config = dcos_launch.config.load_config(launch_config_path)
        if 'installer_url' not in initial_config:
            initial_config['installer_url'] = installer_url
            _write_yaml(initial_config, launch_config_path)


@pytest.fixture(scope='session')
def create_cluster():
    if 'TEST_CREATE_CLUSTER' not in os.environ:
        raise Exception('TEST_CREATE_CLUSTER must be to set true or false in the local environment')
    return os.environ['TEST_CREATE_CLUSTER'] == 'true'


@pytest.fixture(scope='session')
def cluster_info_path(create_cluster):
    path = os.getenv('TEST_CLUSTER_INFO_PATH', 'cluster_info.json')
    if os.path.exists(path) and create_cluster:
        raise Exception('Test cannot begin while cluster_info.json is present in working directory')
    return path


@pytest.fixture(scope='session')
@pytest.mark.skipif(
    'TEST_LAUNCH_CONFIG_PATH' not in os.environ,
    reason='This test must have dcos-launch config YAML or info JSON to run')
def launcher(create_cluster, cluster_info_path):
    """ Optionally create and wait on a cluster to finish provisioning.

    This function uses environment variables as arguments:
    - TEST_LAUNCH_CONFIG_PATH: either a launch config YAML for a new cluster or
        a launch info JSON for an existing cluster
    - TEST_CREATE_CLUSTER: can be `true` or `false`. If `true`, a new cluster
        will be created for this test
    - TEST_CLUSTER_INFO_PATH: path where the cluster info JSON will be dumped
        for future manipulation
    """
    # Use non-strict validation so that info JSONs with extra fields do not
    # raise errors on configuration validation
    launch_config_path = os.environ['TEST_LAUNCH_CONFIG_PATH']
    init_runtime_config(os.getenv('TEST_UPGRADE_PRESETS', 'upgrade'))
    if create_cluster:
        _merge_launch_config_with_env(launch_config_path)
        launcher = dcos_launch.get_launcher(dcos_launch.config.get_validated_config_from_path(launch_config_path))
        info = launcher.create()
        with open(cluster_info_path, 'w') as f:
            json.dump(info, f)
        # basic wait to account for initial provisioning delay
        time.sleep(int(os.getenv("INITIAL_SLEEP", "180")))
        launcher.wait()
    else:
        try:
            launcher = dcos_launch.get_launcher(json.load(open(launch_config_path, 'r')))
            launcher.wait()
        except dcos_launch.util.LauncherError:
            raise AssertionError(
                'Cluster creation was not specified with TEST_CREATE_CLUSTER, yet launcher '
                'cannot reach the speficied cluster')
    return launcher
