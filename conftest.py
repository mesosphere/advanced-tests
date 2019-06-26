import json
import logging
import os
import time
import re

from py._code.assertion import AssertionError
from typing import Generator

import retrying

from dcos_test_utils import dcos_api, enterprise, helpers, dcos_cli

import pytest
import retrying

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
        launcher.install_dcos()
    else:
        try:
            launcher = dcos_launch.get_launcher(json.load(open(launch_config_path, 'r')))
            launcher.wait()
            launcher.install_dcos()
        except dcos_launch.util.LauncherError:
            raise AssertionError(
                'Cluster creation was not specified with TEST_CREATE_CLUSTER, yet launcher '
                'cannot reach the speficied cluster')

    log.info("SSH Key for Debugging: '" + launcher.get_bootstrap_ssh_client().key + "'")

    return launcher


@pytest.fixture(scope='session')
def onprem_cluster(launcher):
    if launcher.config['provider'] != 'onprem':
        pytest.skip('Only onprem provider is supported for upgrades!')
    return launcher.get_onprem_cluster()


@pytest.fixture(scope='session')
def is_enterprise():
    return os.getenv('TEST_UPGRADE_ENTERPRISE', 'false') == 'true'


@pytest.fixture(scope='session')
def dcos_api_session(onprem_cluster, launcher, is_enterprise):
    """ The API session for the cluster at the beginning of the upgrade
    This will be used to start tasks and poll the metrics snapshot endpoint
    """
    return make_dcos_api_session(
        onprem_cluster, launcher, is_enterprise, launcher.config['dcos_config'].get('security'))


@pytest.fixture(scope='session')
def new_dcos_cli() -> Generator[dcos_cli.DcosCli, None, None]:
    cli = dcos_cli.DcosCli.new_cli()
    yield cli
    os.remove(cli.path)
    cli.clear_cli_dir()


@pytest.fixture(scope='session')
def dcoscli(
    new_dcos_cli: dcos_cli.DcosCli,
    dcos_api_session
) -> dcos_cli.DcosCli:
    try:
        log.info("Installing the CLI.")
        new_dcos_cli.setup_enterprise(str(dcos_api_session.default_url))
    except AssertionError as error:
        log.info("Overridding assertions discovered in CLI install due to STDERR error in CLI Installer.")
    return new_dcos_cli


def make_dcos_api_session(onprem_cluster, launcher, is_enterprise: bool=False, security_mode=None):
    ssl_enabled = security_mode in ('strict', 'permissive')
    args = {
        'dcos_url': 'http://' + onprem_cluster.masters[0].public_ip,
        'masters': [m.public_ip for m in onprem_cluster.masters],
        'slaves': [m.public_ip for m in onprem_cluster.private_agents],
        'public_slaves': [m.public_ip for m in onprem_cluster.public_agents],
        'auth_user': dcos_api.DcosUser(helpers.CI_CREDENTIALS),
        'exhibitor_admin_password': launcher.config['dcos_config'].get('exhibitor_admin_password')}

    if is_enterprise:
        api_class = enterprise.EnterpriseApiSession
        args['auth_user'] = enterprise.EnterpriseUser(
            os.getenv('DCOS_LOGIN_UNAME', 'bootstrapuser'),
            os.getenv('DCOS_LOGIN_PW', 'deleteme'))
        if ssl_enabled:
            args['dcos_url'] = args['dcos_url'].replace('http', 'https')
    else:
        api_class = dcos_api.DcosApiSession

    return api_class(**args)


@retrying.retry(
    wait_fixed=(1 * 1000),
    stop_max_delay=(120 * 1000),
    retry_on_result=lambda x: not x)
def wait_for_dns(dcos_api_session, hostname):
    """Return True if Mesos-DNS has at least one entry for hostname."""
    hosts = dcos_api_session.get('/mesos_dns/v1/hosts/' + hostname).json()
    return any(h['host'] != '' and h['ip'] != '' for h in hosts)


def find_app_port(config, app_name):
    """ Finds the port associated with the app in haproxy_getconfig.
    This is done through regex pattern matching.
    """
    pattern = re.search(r'{0}(.+?)\n  bind .+:\d+'.format(app_name), config)
    return pattern.group()[-5:]


@retrying.retry(wait_fixed=180000, stop_max_attempt_number=2)
def set_ca_cert_for_session(session):
    if session.default_url.scheme == 'https':
        session.set_ca_cert()
