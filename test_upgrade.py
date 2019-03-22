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
import json
import logging
import os
import pprint

import math
import uuid

from conftest import set_ca_cert_for_session
from dcos_test_utils import dcos_api, enterprise, helpers
import pytest
import retrying
import yaml
from requests import HTTPError

import upgrade
from conftest import wait_for_dns, make_dcos_api_session
from json_job_definitions import spark_consumer_job, spark_producer_job, viptalk_app, viplisten_app, healthcheck_app, dns_app, docker_pod, docker_bridge, docker_host, docker_ippc, ucr_bridge, ucr_hort, ucr_ippc

log = logging.getLogger(__name__)


def get_master_task_state(dcos_api_session, task_id):
    """Returns the JSON blob associated with the task from /master/state."""
    response = dcos_api_session.get('/mesos/master/state')
    response.raise_for_status()
    master_state = response.json()

    for framework in master_state['frameworks']:
        for task in framework['tasks']:
            if task_id in task['id']:
                return task


def app_task_ids(dcos_api_session, app_id):
    """Return a list of Mesos task IDs for app_id's running tasks."""
    assert app_id.startswith('/')
    response = dcos_api_session.marathon.get('/v2/apps' + app_id + '/tasks')
    response.raise_for_status()
    tasks = response.json()['tasks']
    return [task['id'] for task in tasks]


def pod_task_ids(dcos_api_session, pod_id):
    """Return a list of Mesos task IDs for a given pod_id running tasks."""
    assert pod_id.startswith('/')
    response = dcos_api_session.marathon.get('/v2/pods' + pod_id + '::status')
    response.raise_for_status()
    return [container['containerId']
            for instance in response.json()['instances']
            for container in instance['containers']]


def parse_dns_log(dns_log_content):
    """Return a list of (timestamp, status) tuples from dns_log_content."""
    dns_log = [line.strip().split(' ') for line in dns_log_content.strip().split('\n')]
    if any(len(entry) != 2 or entry[1] not in ['SUCCESS', 'FAILURE'] for entry in dns_log):
        message = 'Malformed DNS log.'
        log.debug(message + ' DNS log content:\n' + dns_log_content)
        raise Exception(message)
    return dns_log


@pytest.fixture(scope='session')
def use_pods():
    return os.getenv('TEST_UPGRADE_USE_PODS', 'true') == 'true'


def wait_for_frameworks_to_deploy(dcoscli):
    """Waits for cassandra and kafka to finish deploying"""
    wait_for_individual_framework_to_deploy(dcoscli, "dcos cassandra plan status deploy --json")
    wait_for_individual_framework_to_deploy(dcoscli, "dcos cassandra plan status recovery --json")
    wait_for_individual_framework_to_deploy(dcoscli, "dcos kafka plan status deploy --json")
    wait_for_individual_framework_to_deploy(dcoscli, "dcos kafka plan status recovery --json")


@retrying.retry(wait_fixed=5000, stop_max_delay=600000)
def wait_for_individual_framework_to_deploy(dcoscli, cli_commands):
    """Takes a cli command to run, and waits for the json attribute 'status' to be 'COMPLETE'"""
    cassandra_deploy_json_return_string = json.loads(dcoscli.exec_command(cli_commands.split())[0])
    log.info("Waiting for '" + str(cli_commands).strip() + "' to complete deploying")
    str(cassandra_deploy_json_return_string["status"]) == str("COMPLETE")


@retrying.retry(wait_fixed=5000, stop_max_delay=300000)
def wait_for_spark_job_to_deploy(dcoscli, run_command_response):
    """Takes a spark status name to run, and waits for the response to contain the 'state' of 'TASK RUNNING'"""
    driver_name = str(run_command_response[0])[str(run_command_response[0]).index('driver-'):]
    status_command_response = dcoscli.exec_command(("dcos spark status " + driver_name).split())
    log.info("Waiting for '" + str(driver_name).strip() + "' to complete deploying")
    assert(''.join(status_command_response).find("state: TASK_RUNNING") != -1)


@retrying.retry(wait_fixed=5000, stop_max_delay=300000)
def wait_for_kafka_topic_to_start(dcoscli):
    """Takes a kafka topic, and waits for the topic to appear in kafka's topic list"""
    kafka_topic_list = str(dcoscli.exec_command("dcos kafka topic list".split()))
    log.info("Waiting for the kafka topic 'mytopicC' to complete deploying")
    assert(kafka_topic_list.find("mytopicC") != -1)


@retrying.retry(wait_fixed=5000, stop_max_delay=300000)
def wait_for_kafka_topic_to_start_counting(dcoscli):
    """waits for the kafka topic started by our spark jobs to begin counting words"""
    kafka_job_words = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]
    log.info("Waiting for the kafka topic 'mytopicC' to begin counting words")
    assert(str(kafka_job_words) != "0")


def start_marathonlb_apps(superuser_api_session, docker_bridge, docker_host, docker_ippc, ucr_bridge, ucr_hort, ucr_ippc):
    app_defs = [docker_bridge, docker_host, docker_ippc, ucr_bridge, ucr_hort, ucr_ippc]
    app_ids = []

    for app_def in app_defs:
        app_id = app_def['id']
        app_ids.append(app_id)

        app_name = app_id[1:] if app_id[0] == '/' else app_id
        log.info('{} is being tested.'.format(app_name))

        try:
            superuser_api_session.marathon.deploy_app(app_def)
            superuser_api_session.marathon.wait_for_deployments_complete
        except AssertionError:
            log.info('Install of ' + app_id + ' failed, retrying the install...')
            superuser_api_session.marathon.destroy_app(app_id)

            superuser_api_session.marathon.deploy_app(app_def)
            superuser_api_session.marathon.wait_for_deployments_complete

    return app_ids


def start_spark_jobs(dcoscli, spark_producer_job, spark_consumer_job):
    try:
        spark_producer_response = dcoscli.exec_command_as_shell("dcos spark run --submit-args=" + spark_producer_job)
        wait_for_spark_job_to_deploy(dcoscli, spark_producer_response)
    except AssertionError:
        log.info('Initialization of spark producer job failed, retrying the run...')
        driver_name = str(spark_producer_response[0])[str(spark_producer_response[0]).index('driver-'):]
        dcoscli.exec_command_as_shell("dcos spark kill " + driver_name)
        spark_producer_response = dcoscli.exec_command_as_shell("dcos spark run --submit-args=" + spark_producer_job)
        wait_for_spark_job_to_deploy(dcoscli, spark_producer_response)

    try:
        spark_consumer_response = dcoscli.exec_command_as_shell("dcos spark run --submit-args=" + spark_consumer_job)
        wait_for_spark_job_to_deploy(dcoscli, spark_consumer_response)
    except AssertionError:
        log.info('Initialization of spark consumer job failed, retrying the run...')
        driver_name = str(spark_consumer_response[0])[str(spark_consumer_response[0]).index('driver-'):]
        dcoscli.exec_command_as_shell("dcos spark kill " + driver_name)
        spark_consumer_response = dcoscli.exec_command_as_shell("dcos spark run --submit-args=" + spark_consumer_job)
        wait_for_spark_job_to_deploy(dcoscli, spark_consumer_response)


def start_marathon_apps(dcos_api_session, viplisten_app, viptalk_app, healthcheck_app, dns_app, docker_pod, use_pods):
    # TODO(branden): We ought to be able to deploy these apps concurrently. See
    # https://mesosphere.atlassian.net/browse/DCOS-13360.
    log.info("Launching viplisten_app")
    dcos_api_session.marathon.deploy_app(viplisten_app)
    dcos_api_session.marathon.wait_for_deployments_complete()
    # viptalk app depends on VIP from viplisten app, which may still fail
    # the first try immediately after wait_for_deployments_complete
    dcos_api_session.marathon.deploy_app(viptalk_app, ignore_failed_tasks=True)
    log.info("Launching viptalk_app")
    dcos_api_session.marathon.wait_for_deployments_complete()

    log.info("Launching healthcheck_app")
    dcos_api_session.marathon.deploy_app(healthcheck_app)
    dcos_api_session.marathon.wait_for_deployments_complete()

    log.info("dns_app: '" + str(dns_app) + "'")
    log.info("resolve name: '" + str(dns_app['env']['RESOLVE_NAME']) + "'")

    # This is a hack to make sure we don't deploy dns_app before the name it's
    # trying to resolve is available.
    log.info("Waiting for healthsheck app to launch to launch dns_app...")
    wait_for_dns(dcos_api_session, dns_app['env']['RESOLVE_NAME'])
    log.info("Launching dns_app")
    dcos_api_session.marathon.deploy_app(dns_app, check_health=False)
    dcos_api_session.marathon.wait_for_deployments_complete()

    test_apps = [healthcheck_app, dns_app, viplisten_app, viptalk_app]
    test_app_ids = [app['id'] for app in test_apps]
    app_tasks_start = {app_id: sorted(app_task_ids(dcos_api_session, app_id)) for app_id in test_app_ids}
    tasks_start = {**app_tasks_start}
    test_pods = list()
    test_pod_ids = list()
    if use_pods:
        log.info("Launching docker_pod")
        dcos_api_session.marathon.deploy_pod(docker_pod)
        dcos_api_session.marathon.wait_for_deployments_complete()
        test_pods = [docker_pod]
        test_pod_ids = [pod['id'] for pod in test_pods]
        pod_tasks_start = {pod_id: sorted(pod_task_ids(dcos_api_session, pod_id)) for pod_id in test_pod_ids}
        tasks_start = {**app_tasks_start, **pod_tasks_start}
        for pod in test_pods:
            assert pod['scaling']['instances'] * len(pod['containers']) == len(tasks_start[pod['id']])

    # Marathon apps and pods cannot share IDs, so we merge task lists here.
    log.debug('Test app tasks at start:\n' + pprint.pformat(tasks_start))

    for app in test_apps:
        assert app['instances'] == len(tasks_start[app['id']])

    return test_app_ids, test_pod_ids, tasks_start


def init_main_frameworks(dcos_api_session, dcoscli):
    # Dictionary containing installed framework-ids.
    framework_ids = {}

    # Add essential services for basic run test
    services = {
        'cassandra': {'version': os.environ.get('CASSANDRA_VERSION'), 'option': None},
        'kafka': {'version': os.environ.get('KAFKA_VERSION'), 'option': None},
        'spark': {'version': os.environ.get('SPARK_VERSION'), 'option': {'service': {'use_bootstrap_for_IP_detect': True, 'user': 'root'}}},
        'marathon-lb': {'version': os.environ.get('MARATHON-LB_VERSION'), 'option': None}
    }

    @retrying.retry(wait_fixed=5000, stop_max_delay=40000)
    def install_framework(api_session, framework_package, framework_config):
        log.info("Installing {0} {1} with options: {2}".format(framework_package, framework_config['version'] or "(most recent version)", framework_config['option'] or '(none)'))
        try:
            installed_package = api_session.cosmos.install_package(framework_package, framework_config['version'], framework_config['option'])
        except HTTPError as error:
            log.info("Caught error: '{0}'".format(str(error)))
            if "409" in str(error):
                #log.info("Package is already installed.  Dumping cosmos info and returning generic package name: " + api_session.cosmos.list_packages())
                return "/{0}".format(framework_package)
            else:
                raise error

        return installed_package.json()['appId']

    # Installing the frameworks
    for package, config in services.items():
        framework_ids[package] = install_framework(dcos_api_session, package, config)

    # Waiting for deployments to complete.
    dcos_api_session.marathon.wait_for_deployments_complete()
    for package in framework_ids.keys():
        assert dcos_api_session.marathon.check_app_instances(framework_ids[package], 1, True, False) is True
    log.info("Completed installing required services.")

    # Install the various CLIs for our frameworks
    dcoscli.exec_command("dcos package install cassandra --cli --yes".split())
    dcoscli.exec_command("dcos package install kafka --cli --yes".split())
    dcoscli.exec_command("dcos package install spark --cli --yes".split())

    return framework_ids


@pytest.fixture(scope='session')
def setup_workload(dcos_api_session, dcoscli, viplisten_app, viptalk_app, healthcheck_app, dns_app, docker_pod, use_pods, spark_producer_job, spark_consumer_job, docker_bridge, docker_host, docker_ippc, ucr_bridge, ucr_hort, ucr_ippc):
    set_ca_cert_for_session(dcos_api_session)

    dcos_api_session.wait_for_dcos()

    # Installing dcos-enterprise-cli to start our frameworks, and install various jobs.
    dcos_api_session.cosmos.install_package('dcos-enterprise-cli', None, None)

    framework_ids = init_main_frameworks(dcos_api_session, dcoscli)

    wait_for_frameworks_to_deploy(dcoscli)

    # Run our two spark jobs to exercise all three of our frameworks
    start_spark_jobs(dcoscli, spark_producer_job, spark_consumer_job)

    # Checking whether applications are running without errors.
    for package in framework_ids.keys():
        assert dcos_api_session.marathon.check_app_instances(framework_ids[package], 1, True, False) is True

    # Wait for the kafka topic to show up in kafka's topic list,
    # and then wait for the topic to begin producing the word count
    wait_for_kafka_topic_to_start(dcoscli)
    wait_for_kafka_topic_to_start_counting(dcoscli)

    # Preserve the current quantity of words from the Kafka job so we can compare it later
    kafka_job_words = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]

    # Start apps that rely on marathon-lb
    marathon_app_ids = start_marathonlb_apps(dcos_api_session, docker_bridge, docker_host, docker_ippc, ucr_bridge, ucr_hort, ucr_ippc)

    # Start the marathon apps
    test_app_ids, test_pod_ids, tasks_start = start_marathon_apps(dcos_api_session, viplisten_app, viptalk_app, healthcheck_app, dns_app, docker_pod, use_pods)

    # Save the master's state of the task to compare with
    # the master's view after the upgrade.
    # See this issue for why we check for a difference:
    # https://issues.apache.org/jira/browse/MESOS-1718
    task_state_start = get_master_task_state(dcos_api_session, tasks_start[test_app_ids[0]][0])
    return test_app_ids, test_pod_ids, tasks_start, task_state_start


@pytest.fixture(scope='session')
def upgraded_dcos(dcos_api_session, launcher, setup_workload, onprem_cluster, is_enterprise):
    """ This test is intended to test upgrades between versions so use
    the same config as the original launch
    """
    # Check for previous installation artifacts first
    bootstrap_host = onprem_cluster.bootstrap_host.public_ip
    bootstrap_ssh_client = launcher.get_bootstrap_ssh_client()
    upgrade.reset_bootstrap_host(bootstrap_ssh_client, bootstrap_host)

    log.info("ssh key: '" + bootstrap_ssh_client.key + "'")

    upgrade_config_overrides = dict()
    if 'TEST_UPGRADE_CONFIG_PATH' in os.environ:
        with open(os.environ['TEST_UPGRADE_CONFIG_PATH'], 'r') as f:
            upgrade_config_overrides = yaml.load(f.read())

    upgrade_config = copy.copy(launcher.config['dcos_config'])

    upgrade_config.update({
        'cluster_name': 'My Upgraded DC/OS',
        'bootstrap_url': 'http://' + onprem_cluster.bootstrap_host.private_ip,
        'master_list': [h.private_ip for h in onprem_cluster.masters]})
    upgrade_config.update(upgrade_config_overrides)
    with open(os.path.join(launcher.config['genconf_dir'], 'config.yaml'), 'w') as f:
        yaml.safe_dump(upgrade_config, f)

    with bootstrap_ssh_client.tunnel(bootstrap_host) as tunnel:
        log.info('Setting up upgrade config on bootstrap host')
        bootstrap_home = tunnel.command(['pwd']).decode().strip()
        genconf_dir = os.path.join(bootstrap_home, 'genconf')
        tunnel.command(['mkdir', '-p', genconf_dir])
        # transfer the config file
        tunnel.copy_file(launcher.config['genconf_dir'], bootstrap_home)

    # do the actual upgrade
    upgrade.upgrade_dcos(
        dcos_api_session,
        onprem_cluster,
        bootstrap_ssh_client,
        launcher.get_ssh_client(),
        dcos_api_session.get_version(),
        os.environ['TEST_UPGRADE_INSTALLER_URL'],
        os.environ['TEST_UPGRADE_USE_CHECKS'] == 'true')

    # API object may need to be updated
    upgrade_session = make_dcos_api_session(
        onprem_cluster,
        launcher,
        is_enterprise,
        upgrade_config_overrides.get('security'))

    # use the Auth session from the previous API session
    upgrade_session.session.auth = dcos_api_session.session.auth

    # this can be set after the fact because the upgrade metrics snapshot
    # endpoint is polled with verify=False
    set_ca_cert_for_session(upgrade_session)

    # Now Re-auth with the new session
    upgrade_session.wait_for_dcos()
    return upgrade_session


class TestUpgrade:
    def test_marathon_tasks_survive(self, upgraded_dcos, use_pods, setup_workload, dcos_api_session):
        """
        This test is to verify that certain jobs that we started prior to upgrading have continued to function through
        the upgrade
        :param upgraded_dcos: the upgraded instance of DCOS
        :param use_pods: boolean to determine if we're using docker pods or not
        :param setup_workload: the return from our setup of the workload prior to upgrading
        :param dcos_api_session: the api session connected to our instance of dcos
        """

        test_app_ids, test_pod_ids, tasks_start, task_state_start, kafka_job_words, framework_ids, marathon_app_ids = setup_workload

        for test_app in test_app_ids:
            dcos_api_session.marathon.wait_for_app_deployment(test_app, 1, False, True, 300)
        app_tasks_end = {app_id: sorted(app_task_ids(upgraded_dcos, app_id)) for app_id in test_app_ids}
        tasks_end = {**app_tasks_end}
        if use_pods:
            pod_tasks_end = {pod_id: sorted(pod_task_ids(upgraded_dcos, pod_id)) for pod_id in test_pod_ids}
            tasks_end = {**app_tasks_end, **pod_tasks_end}
        log.debug('Test app tasks at end:\n' + pprint.pformat(tasks_end))
        assert tasks_start == tasks_end

    def test_mesos_task_state_remains_consistent(self, upgraded_dcos, setup_workload):
        # Verifies that every element of 'subset' is present in 'superset'. The idea
        # is that output generated by Mesos can add entries in the JSON but cannot
        # remove existing ones and the values should be consistent between upgrades.
        def is_contained(subset, superset):
            if type(subset) is not type(superset):
                return False
            elif isinstance(subset, dict):
                return all(key in superset.keys() and is_contained(value, superset[key])
                           for key, value in subset.items())
            elif isinstance(subset, list):
                return all(any(is_contained(sub_item, super_item) for super_item in superset) for sub_item in subset)
            elif isinstance(subset, float):
                return math.isclose(subset, superset)
            else:
                return subset == superset

        test_app_ids, test_pod_ids, tasks_start, task_state_start = setup_workload
        task_state_end = get_master_task_state(upgraded_dcos, tasks_start[test_app_ids[0]][0])
        assert is_contained(task_state_start, task_state_end), '{}\n\n{}'.format(task_state_start, task_state_end)

    def test_app_dns_survive(self, upgraded_dcos, dns_app):
        marathon_framework_id = upgraded_dcos.marathon.get('/v2/info').json()['frameworkId']
        dns_app_task = upgraded_dcos.marathon.get('/v2/apps' + dns_app['id'] + '/tasks').json()['tasks'][0]
        dns_log = parse_dns_log(upgraded_dcos.mesos_sandbox_file(
            dns_app_task['slaveId'],
            marathon_framework_id,
            dns_app_task['id'],
            dns_app['env']['DNS_LOG_FILENAME']))
        dns_failure_times = [entry[0] for entry in dns_log if entry[1] != 'SUCCESS']
        assert len(dns_failure_times) <= 180, 'Failed to resolve Marathon app hostname {hostname} at least once' \
                                              'Hostname failed to resolve at these times:\n{failures}'.format(
            hostname=dns_app['env']['RESOLVE_NAME'],
            failures='\n'.join(dns_failure_times))

    def test_cassandra_tasks_survive(self, upgraded_dcos, dcos_api_session, setup_workload, dcoscli):
        """
        This test is to confirm that a combined job utilizing kafka, cassandra, and spark has conitnued to function
        through the upgrade
        :param upgraded_dcos: the upgraded instance of DCOS
        :param dcos_api_session: the api session connected to our instance of dcos
        :param setup_workload: the return from our setup of the workload prior to upgrading
        :param dcoscli: the api session connected to our instance of dcos
        """
        test_app_ids, test_pod_ids, tasks_start, task_state_start, kafka_job_words, framework_ids, marathon_app_ids = setup_workload

        dcos_api_session.marathon.wait_for_deployments_complete()

        # Checking whether applications are running without errors.
        for package in framework_ids.keys():
            assert dcos_api_session.marathon.check_app_instances(framework_ids[package], 1, True, True) is True

        # Get a new word count from kafka to compare to the word count from before the upgrade
        kafka_job_words_post_upgrade = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]

        assert int(kafka_job_words_post_upgrade) > int(kafka_job_words)

    def test_marathonlb_apps_survived(self, upgraded_dcos, dcos_api_session, setup_workload):
        """
        This test is to confirm that certain jobs that utilize marathon-lb have continued to function through the
        upgrade
        :param upgraded_dcos: the upgraded instance of DCOS
        :param dcos_api_session: the api session connected to our instance of dcos
        :param setup_workload: the return from our setup of the workload prior to upgrading
        """
        test_app_ids, test_pod_ids, tasks_start, task_state_start, kafka_job_words, framework_ids, marathon_app_ids = setup_workload

        log.info("Every marathon instance we attempted to run: '" + str(marathon_app_ids) + "'")

        for marathon_app in marathon_app_ids:
            log.info("Testing for maintained running of: " + marathon_app)

            dcos_api_session.marathon.wait_for_app_deployment(marathon_app, 4, True, True, 300)
            assert dcos_api_session.marathon.check_app_instances(marathon_app, 4, True, True)
