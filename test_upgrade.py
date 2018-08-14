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
import pprint
import uuid
from typing import Generator

from dcos_test_utils import dcos_api, enterprise, helpers, dcos_cli
import pytest
import retrying
import yaml
import json
import time

import upgrade
from dcos_test_utils.enterprise import EnterpriseApiSession

log = logging.getLogger(__name__)


TEST_APP_NAME_FMT = 'upgrade-{}'


@pytest.fixture(scope='session')
def viplisten_app():
    return {
        "id": '/' + TEST_APP_NAME_FMT.format('viplisten-' + uuid.uuid4().hex),
        "cmd": '/usr/bin/nc -l -p $PORT0',
        "cpus": 0.1,
        "mem": 32,
        "instances": 1,
        "container": {
            "type": "MESOS",
            "docker": {
              "image": "alpine:3.5"
            }
        },
        'portDefinitions': [{
            'labels': {
                'VIP_0': '/viplisten:5000'
            }
        }],
        "healthChecks": [{
            "protocol": "COMMAND",
            "command": {
                "value": "/usr/bin/nslookup viplisten.marathon.l4lb.thisdcos.directory && pgrep -x /usr/bin/nc"
            },
            "gracePeriodSeconds": 300,
            "intervalSeconds": 60,
            "timeoutSeconds": 20,
            "maxConsecutiveFailures": 10
        }]
    }


@pytest.fixture(scope='session')
def viptalk_app():
    return {
        "id": '/' + TEST_APP_NAME_FMT.format('viptalk-' + uuid.uuid4().hex),
        "cmd": "/usr/bin/nc viplisten.marathon.l4lb.thisdcos.directory 5000 < /dev/zero",
        "cpus": 0.1,
        "mem": 32,
        "instances": 1,
        "container": {
            "type": "MESOS",
            "docker": {
              "image": "alpine:3.5"
            }
        },
        "healthChecks": [{
            "protocol": "COMMAND",
            "command": {
                "value": "pgrep -x /usr/bin/nc && sleep 5 && pgrep -x /usr/bin/nc"
            },
            "gracePeriodSeconds": 300,
            "intervalSeconds": 60,
            "timeoutSeconds": 20,
            "maxConsecutiveFailures": 10
        }]
    }


@pytest.fixture(scope='session')
def healthcheck_app():
    # HTTP healthcheck app to make sure tasks are reachable during the upgrade.
    # If a task fails its healthcheck, Marathon will terminate it and we'll
    # notice it was killed when we check tasks on exit.
    return {
        "id": '/' + TEST_APP_NAME_FMT.format('healthcheck-' + uuid.uuid4().hex),
        "cmd": "python3 -m http.server 8080",
        "cpus": 0.5,
        "mem": 32.0,
        "instances": 1,
        "container": {
            "type": "DOCKER",
            "docker": {
                "image": "python:3",
                "network": "BRIDGE",
                "portMappings": [
                    {"containerPort": 8080, "hostPort": 0}
                ]
            }
        },
        "healthChecks": [
            {
                "protocol": "HTTP",
                "path": "/",
                "portIndex": 0,
                "gracePeriodSeconds": 300,
                "intervalSeconds": 60,
                "timeoutSeconds": 20,
                "maxConsecutiveFailures": 10
            }
        ],
    }


@pytest.fixture(scope='session')
def dns_app(healthcheck_app):
    # DNS resolution app to make sure DNS is available during the upgrade.
    # Periodically resolves the healthcheck app's domain name and logs whether
    # it succeeded to a file in the Mesos sandbox.
    healthcheck_app_id = healthcheck_app['id'].lstrip('/')
    return {
        "id": '/' + TEST_APP_NAME_FMT.format('dns-' + uuid.uuid4().hex),
        "cmd": """
while true
do
    printf "%s " $(date --utc -Iseconds) >> $MESOS_SANDBOX/$DNS_LOG_FILENAME
    if host -W $TIMEOUT_SECONDS $RESOLVE_NAME
    then
        echo SUCCESS >> $MESOS_SANDBOX/$DNS_LOG_FILENAME
    else
        echo FAILURE >> $MESOS_SANDBOX/$DNS_LOG_FILENAME
    fi
    sleep $INTERVAL_SECONDS
done
""",
        "env": {
            'RESOLVE_NAME': helpers.marathon_app_id_to_mesos_dns_subdomain(healthcheck_app_id) + '.marathon.mesos',
            'DNS_LOG_FILENAME': 'dns_resolve_log.txt',
            'INTERVAL_SECONDS': '1',
            'TIMEOUT_SECONDS': '1',
        },
        "cpus": 0.5,
        "mem": 32.0,
        "instances": 1,
        "container": {
            "type": "DOCKER",
            "docker": {
                "image": "branden/bind-utils",
                "network": "BRIDGE",
            }
        },
        "dependencies": [healthcheck_app_id],
    }


@pytest.fixture(scope='session')
def docker_pod():
    return {
        'id': '/' + TEST_APP_NAME_FMT.format('docker-pod-' + uuid.uuid4().hex),
        'scaling': {'kind': 'fixed', 'instances': 1},
        'environment': {'PING': 'PONG'},
        'containers': [
            {
                'name': 'container1',
                'resources': {'cpus': 0.1, 'mem': 32},
                'image': {'kind': 'DOCKER', 'id': 'debian:jessie'},
                'exec': {'command': {'shell': 'while true; do sleep 1; done'}},
                'healthcheck': {'command': {'shell': 'sleep 1'}}
            },
            {
                'name': 'container2',
                'resources': {'cpus': 0.1, 'mem': 32},
                'exec': {'command': {'shell': 'echo $PING > foo; while true; do sleep 1; done'}},
                'healthcheck': {'command': {'shell': 'test $PING = `cat foo`'}}
            }
        ],
        'networks': [{'mode': 'host'}]
    }

@pytest.fixture(scope='session')
def spark_producer_job():
    return '"--conf spark.mesos.containerizer=mesos --conf spark.scheduler.maxRegisteredResourcesWaitingTime=2400s --conf spark.scheduler.minRegisteredResourcesRatio=1.0 --conf spark.cores.max=2 --conf spark.executor.cores=2 --conf spark.executor.mem=2g --conf spark.driver.mem=2g --class KafkaRandomFeeder http://infinity-artifacts.s3.amazonaws.com/scale-tests/dcos-spark-scala-tests-assembly-20180523-fa29ab5.jar --appName Producer --brokers kafka-0-broker.kafka.autoip.dcos.thisdcos.directory:1025,kafka-1-broker.kafka.autoip.dcos.thisdcos.directory:1025,kafka-2-broker.kafka.autoip.dcos.thisdcos.directory:1025 --topics mytopicC --numberOfWords 3600 --wordsPerSecond 1"'

@pytest.fixture(scope='session')
def spark_consumer_job():
    return '"--conf spark.mesos.containerizer=mesos --conf spark.scheduler.maxRegisteredResourcesWaitingTime=2400s --conf spark.scheduler.minRegisteredResourcesRatio=1.0 --conf spark.cores.max=1 --conf spark.executor.cores=1 --conf spark.executor.mem=2g --conf spark.driver.mem=2g --conf spark.cassandra.connection.host=node-0-server.cassandra.autoip.dcos.thisdcos.directory --conf spark.cassandra.connection.port=9042 --class KafkaWordCount http://infinity-artifacts.s3.amazonaws.com/scale-tests/dcos-spark-scala-tests-assembly-20180523-fa29ab5.jar --appName Consumer --brokers kafka-0-broker.kafka.autoip.dcos.thisdcos.directory:1025,kafka-1-broker.kafka.autoip.dcos.thisdcos.directory:1025,kafka-2-broker.kafka.autoip.dcos.thisdcos.directory:1025 --topics mytopicC --groupId group1 --batchSizeSeconds 10 --cassandraKeyspace mykeyspace --cassandraTable mytable"'


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
    new_dcos_cli.setup_enterprise(str(dcos_api_session.default_url))
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

def wait_for_individual_framework_to_deploy(dcoscli, cli_commands):
    """Takes a cli command to run, and waits for the json attribute 'status' to be 'COMPLETE'"""
    cassandra_deploy_json_return_string = json.loads(
        dcoscli.exec_command(cli_commands.split())[0])

    count = 0

    while (str(cassandra_deploy_json_return_string["status"]) != str("COMPLETE") and count <= 120):
        log.info("Waiting for '" + str(cli_commands).strip() + "' to complete deploying - Attempt: " + str(count))
        time.sleep(5)
        cassandra_deploy_json_return_string = json.loads(
            dcoscli.exec_command(cli_commands.split())[0])
        count += 1

def wait_for_spark_job_to_deploy(dcoscli, run_command_response):
    """Takes a spark status name to run, and waits for the response to contain the 'state' of 'TASK RUNNING'"""
    driver_name = str(run_command_response[0])[str(run_command_response[0]).index('driver-'):]

    status_command_response = dcoscli.exec_command(("dcos spark status " + driver_name).split())

    count = 0

    while (''.join(status_command_response).find("state: TASK_RUNNING") == -1 and count <= 36):
        log.info("Waiting for '" + str(driver_name).strip() + "' to complete deploying - Attempt: " + str(count))
        time.sleep(5)
        status_command_response = dcoscli.exec_command(("dcos spark status " + driver_name).split())
        count += 1

def wait_for_kafka_topic_to_start(dcoscli):
    """Takes a kafka topic, and waits for the topic to appear in kafka's topic list"""
    kafka_topic_list = str(dcoscli.exec_command("dcos kafka topic list".split()))

    count = 0

    while (kafka_topic_list.find("mytopicC") == -1 and count <= 60):
        log.info("Waiting for the kafka topic 'mytopicC' to complete deploying - Attempt: " + str(count))
        time.sleep(5)
        kafka_topic_list = str(dcoscli.exec_command("dcos kafka topic list".split()))
        count += 1

def wait_for_kafka_topic_to_start_counting(dcoscli):
    """waits for the kafka topic started by our spark jobs to begin counting words"""
    kafka_job_words = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]

    count = 0

    log.info("Recieved: '" + kafka_job_words + "' which is equal to 0: '" + str(kafka_job_words) == "0" + "'")

    while (str(kafka_job_words) == "0" and count <= 60):
        log.info("Waiting for the kafka topic 'mytopicC' to begin counting words - Attempt: " + str(count))
        time.sleep(5)
        kafka_job_words = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]
        count += 1


@pytest.fixture(scope='session')
def setup_workload(dcos_api_session, dcoscli, viptalk_app, viplisten_app, healthcheck_app, dns_app, docker_pod, use_pods):
    if dcos_api_session.default_url.scheme == 'https':
        dcos_api_session.set_ca_cert()
    dcos_api_session.wait_for_dcos()

    # Installing dcos-enterprise-cli to start our frameworks, and install various jobs.
    dcos_api_session.cosmos.install_package('dcos-enterprise-cli', None, None)

    # Dictionary containing installed framework-ids.
    framework_ids = {}

    # Add essential services for basic run test
    services = {
        'cassandra': {'version': os.environ.get('CASSANDRA_VERSION'), 'option': None},
        'kafka': {'version': os.environ.get('KAFKA_VERSION'), 'option': None},
        'spark': {'version': os.environ.get('SPARK_VERSION'), 'option': None}
    }

    #Installing the frameworks
    for package, config in services.items():
        installed_package = dcos_api_session.cosmos.install_package(package, config['version'], config['option'])
        log.info("Installing {0} {1}".format(package, config['version'] or "(most recent version)"))

        framework_ids[package] = installed_package.json()['appId']

    # Waiting for deployments to complete.
    dcos_api_session.marathon.wait_for_deployments_complete()
    log.info("Completed installing required services.")

    #Install our various CLIs
    dcoscli.exec_command("dcos package install cassandra --cli --yes".split())
    dcoscli.exec_command("dcos package install kafka --cli --yes".split())
    dcoscli.exec_command("dcos package install spark --cli --yes".split())

    wait_for_frameworks_to_deploy(dcoscli)

    # Run our two spark jobs to exercise all three of our frameworks
    spark_producer_response = dcoscli.exec_command_as_shell("dcos spark run --submit-args=" + spark_producer_job())
    wait_for_spark_job_to_deploy(dcoscli, spark_producer_response)

    spark_consumer_response = dcoscli.exec_command_as_shell("dcos spark run --submit-args=" + spark_consumer_job())
    wait_for_spark_job_to_deploy(dcoscli, spark_consumer_response)

    # Checking whether applications are running without errors.
    for package in framework_ids.keys():
        assert dcos_api_session.marathon.check_app_instances(framework_ids[package], 1, True, False) is True

    # Wait for the kafka topic to show up in kafka's topic list, and then wait for the topic to begin producing the word count
    wait_for_kafka_topic_to_start(dcoscli)
    wait_for_kafka_topic_to_start_counting(dcoscli)

    # Preserve the current quantity of words from the Kafka job so we can compare it later
    kafka_job_words = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]

    # TODO(branden): We ought to be able to deploy these apps concurrently. See
    # https://mesosphere.atlassian.net/browse/DCOS-13360.
    dcos_api_session.marathon.deploy_app(viplisten_app)
    dcos_api_session.marathon.wait_for_deployments_complete()
    # viptalk app depends on VIP from viplisten app, which may still fail
    # the first try immediately after wait_for_deployments_complete
    dcos_api_session.marathon.deploy_app(viptalk_app, ignore_failed_tasks=True)
    dcos_api_session.marathon.wait_for_deployments_complete()

    dcos_api_session.marathon.deploy_app(healthcheck_app)
    dcos_api_session.marathon.wait_for_deployments_complete()
    # This is a hack to make sure we don't deploy dns_app before the name it's
    # trying to resolve is available.
    wait_for_dns(dcos_api_session, dns_app['env']['RESOLVE_NAME'])
    dcos_api_session.marathon.deploy_app(dns_app, check_health=False)
    dcos_api_session.marathon.wait_for_deployments_complete()

    test_apps = [healthcheck_app, dns_app, viplisten_app, viptalk_app]
    test_app_ids = [app['id'] for app in test_apps]
    app_tasks_start = {app_id: sorted(app_task_ids(dcos_api_session, app_id)) for app_id in test_app_ids}
    tasks_start = {**app_tasks_start}
    test_pods = list()
    test_pod_ids = list()
    if use_pods:
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

    # Save the master's state of the task to compare with
    # the master's view after the upgrade.
    # See this issue for why we check for a difference:
    # https://issues.apache.org/jira/browse/MESOS-1718
    task_state_start = get_master_task_state(dcos_api_session, tasks_start[test_app_ids[0]][0])
    return test_app_ids, test_pod_ids, tasks_start, task_state_start, kafka_job_words, framework_ids


@pytest.fixture(scope='session')
def upgraded_dcos(dcos_api_session, launcher, setup_workload, onprem_cluster, is_enterprise):
    """ This test is intended to test upgrades between versions so use
    the same config as the original launch
    """
    # Check for previous installation artifacts first
    bootstrap_host = onprem_cluster.bootstrap_host.public_ip
    bootstrap_ssh_client = launcher.get_bootstrap_ssh_client()
    upgrade.reset_bootstrap_host(bootstrap_ssh_client, bootstrap_host)

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
    if upgrade_session.default_url.scheme == 'https':
        upgrade_session.set_ca_cert()

    # Now Re-auth with the new session
    upgrade_session.wait_for_dcos()
    return upgrade_session


class TestUpgrade:
    def test_marathon_tasks_survive(self, upgraded_dcos, use_pods, setup_workload):
        test_app_ids, test_pod_ids, tasks_start, _ = setup_workload
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

    @pytest.mark.xfail
    def test_app_dns_survive(self, upgraded_dcos, dns_app):
        marathon_framework_id = upgraded_dcos.marathon.get('/v2/info').json()['frameworkId']
        dns_app_task = upgraded_dcos.marathon.get('/v2/apps' + dns_app['id'] + '/tasks').json()['tasks'][0]
        dns_log = parse_dns_log(upgraded_dcos.mesos_sandbox_file(
            dns_app_task['slaveId'],
            marathon_framework_id,
            dns_app_task['id'],
            dns_app['env']['DNS_LOG_FILENAME']))
        dns_failure_times = [entry[0] for entry in dns_log if entry[1] != 'SUCCESS']
        assert len(dns_failure_times) == 0, 'Failed to resolve Marathon app hostname {hostname} at least once' \
            'Hostname failed to resolve at these times:\n{failures}'.format(
                hostname=dns_app['env']['RESOLVE_NAME'],
                failures='\n'.join(dns_failure_times))

    def test_cassandra_tasks_survive(self, dcos_api_session, setup_workload, dcoscli):
        test_app_ids, test_pod_ids, tasks_start, task_state_start, kafka_job_words, framework_ids = setup_workload

        # Checking whether applications are running without errors.
        for package in framework_ids.keys():
            assert dcos_api_session.marathon.check_app_instances(framework_ids[package], 1, True, False) is True

    # Get a new word count from kafka to compare to the word count from before the upgrade
        kafka_job_words_post_upgrade = json.loads(dcoscli.exec_command("dcos kafka topic offsets mytopicC".split())[0])[0]["0"]

        assert kafka_job_words_post_upgrade > kafka_job_words
