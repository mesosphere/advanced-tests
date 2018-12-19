"""

WARNING: This is a destructive process and you cannot go back

=========================================================
This test has 5 input parameters (set in the environment)
=========================================================
Required:
  TEST_LAUNCH_CONFIG_PATH: path to a dcos-launch config for the cluster that will be upgraded.
      This cluster may or may not exist yet
  TEST_UPGRADE_INSTALLER_URL: The installer pulled from this URL will upgrade the aforementioned cluster.
Optional
  TEST_CREATE_CLUSTER: if set to `true`, a cluster will be created. Otherwise it will be assumed
      the provided launch config is a dcos-launch artifact
  TEST_UPGRADE_CONFIG_PATH: path to a YAML file for injecting parameters into the config to be
      used in generating the upgrade script
  TEST_UPGRADE_USE_CHECKS: if set to `true`, 3dt checks will be run to verify that a node upgrade was
      successful
  TEST_UPGRADE_USE_PODS: if set to `true`, test will launch a marathon pod in addition to the app
      before the upgrade begins
"""
import copy
import logging
import math
import os
import itertools
import time
import pprint
import uuid

from dcos_test_utils import dcos_api, enterprise, helpers
import pytest
import retrying
import yaml

import upgrade

log = logging.getLogger(__name__)


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


@pytest.fixture(scope='session')
def use_pods():
    return os.getenv('TEST_UPGRADE_USE_PODS', 'true') == 'true'

@pytest.fixture(scope='session')
def hello_world_app():
    return {
        "id": "/helloworld",
        "cpus": 1,
        "mem": 1024,
        "instances": 1,
        "container": {
            "type": "DOCKER",
            "volumes": [],
            "docker": {
                "image": "docker-private.mesosphere.com/hello-world:latest",
                "forcePullImage": False,
                "privileged": False,
                "parameters": []
            }
        },
        "fetch": [
            {
                "uri": "file:///etc/docker.tar.gz"
            }
        ]
    }


@pytest.fixture(scope='session')
def enable_private_docker_registry(dcos_api_session, launcher, onprem_cluster, hello_world_app):
    bootstrap_ssh_client = launcher.get_bootstrap_ssh_client()

    log.info("Collecting Master, and Node lists to add docker registry access")

    master_ips = [m.public_ip for m in onprem_cluster.masters]
    private_ips = [m.public_ip for m in onprem_cluster.private_agents]
    publics_ips = [m.public_ip for m in onprem_cluster.public_agents]

    for node_host in itertools.chain(master_ips, private_ips, publics_ips):
        with bootstrap_ssh_client.tunnel(node_host) as tunnel:
            log.info("Adding docker registry access to: " + node_host)

            tunnel.command("sudo usermod -a -G docker $USER".split())

            tunnel.command("docker login -u mesosphere-ci -p k7DRiu4d3W7e0eP8 docker-private.mesosphere.com".split())
            tunnel.command("sudo tar -czf docker.tar.gz .docker".split())
            tunnel.command("sudo tar -tvf ~/docker.tar.gz".split())

            tunnel.command("sudo cp docker.tar.gz /etc/".split())

    if dcos_api_session.default_url.scheme == 'https':
        dcos_api_session.set_ca_cert()
    dcos_api_session.wait_for_dcos()

    log.info("Creating Hello World App to test docker registry access")

    dcos_api_session.marathon.deploy_app(hello_world_app)
    dcos_api_session.marathon.wait_for_deployments_complete()


class TestPrivateDockerRegistry:
    def test_private_docker_registry(self, enable_private_docker_registry):
        time.sleep(50000)
        assert 1 == 1
