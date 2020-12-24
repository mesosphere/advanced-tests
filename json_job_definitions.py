import uuid
import pytest

from dcos_test_utils import helpers

TEST_APP_NAME_FMT = 'upgrade-{}'


@pytest.fixture(scope='session')
def viplisten_app():
    service_name = TEST_APP_NAME_FMT.format('viplisten-' + uuid.uuid4().hex)

    return {
        "id": '/' + service_name,
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
                "value": "/usr/bin/nslookup " + service_name + ".marathon.autoip.dcos.thisdcos.directory && pgrep -x /usr/bin/nc"
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
    return '"--conf spark.mesos.containerizer=mesos --conf spark.scheduler.maxRegisteredResourcesWaitingTime=2400s ' \
           '--conf spark.scheduler.minRegisteredResourcesRatio=1.0 --conf spark.cores.max=2 --conf ' \
           'spark.executor.cores=2 --conf spark.executor.mem=2g --conf spark.driver.mem=2g --class KafkaRandomFeeder ' \
           'http://infinity-artifacts.s3.amazonaws.com/scale-tests/dcos-spark-scala-tests-assembly-3.0.1-20201223-b06cb01.jar' \
           '.jar --appName Producer --brokers kafka-0-broker.kafka.autoip.dcos.thisdcos.directory:1025,' \
           'kafka-1-broker.kafka.autoip.dcos.thisdcos.directory:1025,' \
           'kafka-2-broker.kafka.autoip.dcos.thisdcos.directory:1025 --topics mytopicC --numberOfWords 3600 ' \
           '--wordsPerSecond 1" '


@pytest.fixture(scope='session')
def spark_consumer_job():
    return '"--conf spark.mesos.containerizer=mesos --conf spark.scheduler.maxRegisteredResourcesWaitingTime=2400s --conf spark.scheduler.minRegisteredResourcesRatio=1.0 --conf spark.cores.max=1 --conf spark.executor.cores=1 --conf spark.executor.mem=2g --conf spark.driver.mem=2g --conf spark.cassandra.connection.host=node-0-server.cassandra.autoip.dcos.thisdcos.directory --conf spark.cassandra.connection.port=9042 --class KafkaWordCount http://infinity-artifacts.s3.amazonaws.com/scale-tests/dcos-spark-scala-tests-assembly-3.0.1-20201223-b06cb01.jar --appName Consumer --brokers kafka-0-broker.kafka.autoip.dcos.thisdcos.directory:1025,kafka-1-broker.kafka.autoip.dcos.thisdcos.directory:1025,kafka-2-broker.kafka.autoip.dcos.thisdcos.directory:1025 --topics mytopicC --groupId group1 --batchSizeSeconds 10 --cassandraKeyspace mykeyspace --cassandraTable mytable"'


@pytest.fixture(scope='session')
def docker_bridge():
    return {
        "id": "/nginx-docker-bridge",
        "user": "root",
        "cmd": "echo 'nginx-docker-bridge' > /usr/share/nginx/html/index.html; nginx -g 'daemon off;'",
        "container": {
            "portMappings": [
                {
                    "containerPort": 80,
                    "hostPort": 0,
                    "protocol": "tcp"
                }
            ],
            "type": "DOCKER",
            "volumes": [],
            "docker": {
                "image": "nginx"
            }
        },
        "cpus": 0.1,
        "instances": 4,
        "labels": {
            "HAPROXY_GROUP": "external",
            "HAPROXY_0_PORT": "10100",
            "HAPROXY_0_VHOST": "nginx-docker-bridge.test",
            "HAPROXY_0_ENABLED": "true"
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
        "mem": 32,
        "networks": [
            {
                "mode": "container/bridge"
            }
        ],
        "requirePorts": False
    }


@pytest.fixture(scope='session')
def docker_host():
    return {
        "id": "/nginx-docker-host",
        "user":"root",
        "cmd": "sed -i \"s/80/${PORT0}/\" /etc/nginx/conf.d/default.conf; echo 'nginx-docker-host' > /usr/share/nginx/html/index.html; nginx -g 'daemon off;'",
        "container": {
            "type": "DOCKER",
            "volumes": [],
            "docker": {
                "image": "nginx",
                "network": "HOST"
            }
        },
        "cpus": 0.1,
        "disk": 0,
        "instances": 4,
        "mem": 32,
        "requirePorts": False,
        "labels": {
            "HAPROXY_GROUP": "external",
            "HAPROXY_0_PORT": "10300",
            "HAPROXY_0_VHOST": "nginx-docker-host.test",
            "HAPROXY_0_ENABLED": "true"
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
        "fetch": [],
        "constraints": [],
        "portDefinitions": [
            {
                "protocol": "tcp",
                "port": 0
            }
        ]
    }


@pytest.fixture(scope='session')
def docker_ippc():
    return {
        "id": "/nginx-docker-ippc",
        "user":"root",
        "cmd": "echo 'nginx-docker-ippc' > /usr/share/nginx/html/index.html; nginx -g 'daemon off;'",
        "cpus": 0.1,
        "mem": 32,
        "disk": 0,
        "instances": 4,
        "container": {
            "type": "DOCKER",
            "volumes": [],
            "docker": {
                "image": "nginx",
                "parameters": []
            },
            "portMappings": [
                {
                    "containerPort": 80
                }
            ]
        },
        "requirePorts": False,
        "labels": {
            "HAPROXY_GROUP": "external",
            "HAPROXY_0_PORT": "10200",
            "HAPROXY_0_VHOST": "nginx-docker-ippc.test",
            "HAPROXY_0_ENABLED": "true"
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
        "ipAddress": {
            "groups": [],
            "networkName": "dcos"
        }
    }


@pytest.fixture(scope='session')
def ucr_bridge():
    return {
        "id": "/nginx-ucr-bridge",
        "user":"root",
        "cmd": "echo 'nginx-ucr-bridge' > /usr/share/nginx/html/index.html; nginx -g 'daemon off;'",
        "container": {
            "portMappings": [
                {
                    "containerPort": 80,
                    "hostPort": 0,
                    "protocol": "tcp"
                }
            ],
            "type": "MESOS",
            "volumes": [],
            "docker": {
                "image": "nginx"
            }
        },
        "cpus": 0.1,
        "disk": 0,
        "instances": 4,
        "mem": 32,
        "requirePorts": False,
        "labels": {
            "HAPROXY_GROUP": "external",
            "HAPROXY_0_PORT": "10500",
            "HAPROXY_0_VHOST": "nginx-ucr-bridge.test",
            "HAPROXY_0_ENABLED": "true"
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
        "networks": [
            {
                "mode": "container/bridge"
            }
        ]
    }


@pytest.fixture(scope='session')
def ucr_hort():
    return {
        "id": "/nginx-ucr-host",
        "user":"root",
        "backoffFactor": 1.15,
        "backoffSeconds": 1,
        "cmd": "sed -i \"s/80/${PORT0}/\" /etc/nginx/conf.d/default.conf; echo 'nginx-ucr-host' > /usr/share/nginx/html/index.html; nginx -g 'daemon off;'",
        "container": {
            "type": "MESOS",
            "volumes": [],
            "docker": {
                "image": "nginx",
                "forcePullImage": False,
                "parameters": []
            }
        },
        "cpus": 0.1,
        "disk": 0,
        "instances": 4,
        "labels": {
            "HAPROXY_GROUP": "external",
            "HAPROXY_0_PORT": "10400",
            "HAPROXY_0_VHOST": "nginx-ucr-host.test",
            "HAPROXY_0_ENABLED": "true"
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
        "maxLaunchDelaySeconds": 3600,
        "mem": 32,
        "gpus": 0,
        "networks": [
            {
                "mode": "host"
            }
        ],
        "portDefinitions": [
            {
                "protocol": "tcp",
                "port": 10003
            }
        ]
    }


@pytest.fixture(scope='session')
def ucr_ippc():
    return {
        "id": "/nginx-ucr-ippc",
        "user":"root",
        "backoffFactor": 1.15,
        "backoffSeconds": 1,
        "cmd": "echo 'nginx-ucr-ippc' > /usr/share/nginx/html/index.html; nginx -g 'daemon off;'",
        "container": {
            "type": "MESOS",
            "volumes": [],
            "docker": {
                "image": "nginx",
                "forcePullImage": False,
                "parameters": []
            }
        },
        "cpus": 0.1,
        "disk": 0,
        "instances": 4,
        "labels": {
            "HAPROXY_GROUP": "external",
            "HAPROXY_0_PORT": "10600",
            "HAPROXY_0_VHOST": "nginx-ucr-ippc.test",
            "HAPROXY_0_BACKEND_SERVER_OPTIONS": "  server {serverName} {host_ipv4}:80 {cookieOptions}{healthCheckOptions}{otherOptions}\n",
            "HAPROXY_0_ENABLED": "true"
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
        "mem": 32,
        "gpus": 0,
        "networks": [
            {
                "name": "dcos",
                "mode": "container"
            }
        ]
    }
