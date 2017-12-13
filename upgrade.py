import json
import logging
import os
import sys
import random

import retrying

from dcos_launch.platforms import onprem as platforms_onprem
from dcos_test_utils import dcos_api, onprem, ssh_client

log = logging.getLogger(__name__)


@retrying.retry(
    wait_fixed=1000 * 10,
    retry_on_result=lambda result: result is False)
def wait_for_mesos_metric(cluster, host_public, key, bootstrap, host_private, ssh):
    """Return True when host's Mesos metric key is equal to value."""
    log.info('Polling metrics snapshot endpoint')
    if host_public in cluster.masters:
        port = "5050"
    else:
        port = "5051"

    auth_str = cluster.auth_user.auth_header['Authorization']
    curl_cmd = [
        'curl', '--insecure',
        '-H', 'Authorization:' + auth_str,
        cluster.default_url.scheme + '://' + host_private + ":" + port + '/metrics/snapshot']
    with ssh.tunnel(bootstrap) as t:
        response = json.loads(t.command(curl_cmd).decode('utf-8'))
    return response[key] == 1


def reset_bootstrap_host(ssh: ssh_client.SshClient, bootstrap_host: str):
    with ssh.tunnel(bootstrap_host) as t:
        log.info('Checking for previous installer before starting upgrade')
        home_dir = t.command(['pwd']).decode().strip()
        previous_installer = t.command(
            ['sudo', 'docker', 'ps', '--quiet', '--filter', 'name=dcos-genconf']).decode().strip()
        if previous_installer:
            log.info('Previous installer found, killing...')
            t.command(['sudo', 'docker', 'rm', '--force', previous_installer])
        t.command(['sudo', 'rm', '-rf', os.path.join(home_dir, 'genconf*'), os.path.join(home_dir, 'dcos*')])


def upgrade_dcos(
        dcos_api_session: dcos_api.DcosApiSession,
        onprem_cluster: onprem.OnpremCluster,
        bootstrap_client: ssh_client.SshClient,
        node_client: ssh_client.SshClient,
        starting_version: str,
        installer_url: str,
        use_checks: bool) -> None:
    """ Performs the documented upgrade process on a cluster

    (1) downloads installer
    (2) runs the --node-upgrade command
    (3) edits the upgrade script to allow docker to live
    (4) (a) goes to each host and starts the upgrade procedure
        (b) uses an API session to check the upgrade endpoint

    Note:
        - This is intended for testing purposes only and is an irreversible process
        - One must have all file-based resources on the bootstrap host before
            invoking this function

    Args:
        dcos_api_session: API session object capable of authenticating with the
            upgraded DC/OS cluster
        onprem_cluster: abstraction for the cluster to be upgraded
        installer_url: URL for the installer to drive the upgrade

    TODO: This method is only supported when the installer has the node upgrade script
        feature which was not added until 1.9. Thus, add steps to do a 1.8 -> 1.8 upgrade
    """
    bootstrap_host = onprem_cluster.bootstrap_host.public_ip

    # Fetch installer
    bootstrap_home = bootstrap_client.get_home_dir(bootstrap_host)
    installer_path = os.path.join(bootstrap_home, 'dcos_generate_config.sh')
    onprem.download_dcos_installer(bootstrap_client, bootstrap_host, installer_path, installer_url)

    with bootstrap_client.tunnel(bootstrap_host) as tunnel:
        log.info('Generating node upgrade script')
        tunnel.command(['bash', installer_path, '--help'])
        upgrade_version = json.loads(tunnel.command(
            ['bash', installer_path, '--version']).decode('utf-8'))['version']
        use_node_upgrade_script = not upgrade_version.startswith('1.8')
        if use_node_upgrade_script:
            upgrade_script_url = tunnel.command(
                ['bash', installer_path, '--generate-node-upgrade-script ' + starting_version]
            ).decode('utf-8').splitlines()[-1].split("Node upgrade script URL: ", 1)[1]
        else:
            tunnel.command(['bash', installer_path, '--genconf'])
            upgrade_script_url = 'http://' + bootstrap_host + '/dcos_install.sh'

        log.info('Editing node upgrade script...')
        # Remove docker (and associated journald) restart from the install
        # script. This prevents Docker-containerized tasks from being killed
        # during agent upgrades.
        tunnel.command([
            'sudo', 'sed', '-i',
            '-e', '"s/systemctl restart systemd-journald//g"',
            '-e', '"s/systemctl restart docker//g"',
            bootstrap_home + '/genconf/serve/dcos_install.sh'])
        nginx_name = 'dcos-bootstrap-nginx'
        volume_mount = os.path.join(os.path.dirname(installer_path), 'genconf/serve') + ':/usr/share/nginx/html'
        if platforms_onprem.get_docker_service_status(tunnel, nginx_name):
            tunnel.command(['sudo', 'docker', 'rm', '-f', nginx_name])
        platforms_onprem.start_docker_service(
            tunnel, nginx_name,
            ['--publish=80:80', '--volume=' + volume_mount, 'nginx'])
    # upgrading can finally start
    master_list = onprem_cluster.masters
    private_agent_list = onprem_cluster.private_agents
    public_agent_list = onprem_cluster.public_agents
    upgrade_ordering = [
        # Upgrade masters in a random order.
        ('master', 'master', random.sample(master_list, len(master_list))),
        ('slave', 'agent', private_agent_list),
        ('slave_public', 'public agent', public_agent_list)]
    logging.info('\n'.join(
        ['Upgrade plan:'] +
        ['{} ({})'.format(host.public_ip, role_name) for _, role_name, hosts in upgrade_ordering for host in hosts]
    ))
    for role, role_name, hosts in upgrade_ordering:
        log.info('Upgrading {} nodes: {}'.format(role_name, repr(hosts)))
        for host in hosts:
            log.info('Upgrading {}: {}'.format(role_name, repr(host.public_ip)))
            node_client.command(
                host.public_ip,
                [
                    'curl',
                    '--silent',
                    '--verbose',
                    '--show-error',
                    '--fail',
                    '--location',
                    '--keepalive-time', '2',
                    '--retry', '20',
                    '--speed-limit', '100000',
                    '--speed-time', '60',
                    '--remote-name', upgrade_script_url])
            log.info("Starting upgrade script on {host} ({role_name})...".format(
                host=host.public_ip, role_name=role_name))
            if use_checks:
                # use checks is implicit in this command. The upgrade is
                # completely contained to this step
                node_client.command(host.public_ip, ['sudo', 'bash', 'dcos_node_upgrade.sh'], stdout=sys.stdout.buffer)
            else:
                # If not using the dcoc-checks service, polling endpoints is
                # required in order to pace the upgrade to persist state.
                if use_node_upgrade_script:
                    if upgrade_version.startswith('1.1'):
                        # checks are implicit and must be disabled in 1.10 and above
                        node_client.command(host.public_ip, ['sudo', 'bash', 'dcos_node_upgrade.sh', '--skip-checks'])
                    else:
                        # older installer have no concepts of checks
                        node_client.command(host.public_ip, ['sudo', 'bash', 'dcos_node_upgrade.sh'])
                else:
                    # no upgrade script to invoke, do upgrade manually
                    node_client.command(
                        host.public_ip, ['sudo', '-i', '/opt/mesosphere/bin/pkgpanda', 'uninstall'])
                    node_client.command(
                        host.public_ip, ['sudo', 'rm', '-rf', '/opt/mesosphere', '/etc/mesosphere'])
                    node_client.command(
                        host.public_ip, ['sudo', 'bash', 'dcos_install.sh', '-d', role])
                wait_metric = {
                    'master': 'registrar/log/recovered',
                    'slave': 'slave/registered',
                    'slave_public': 'slave/registered',
                }[role]
                log.info('Waiting for {} to rejoin the cluster...'.format(role_name))
                try:
                    wait_for_mesos_metric(
                        dcos_api_session, host.public_ip, wait_metric,
                        bootstrap_host, host.private_ip, ssh_client)
                except retrying.RetryError as exc:
                    raise Exception(
                        'Timed out waiting for {} to rejoin the cluster after upgrade: {}'.
                        format(role_name, repr(host.public_ip))
                    ) from exc
