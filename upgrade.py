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
def wait_for_mesos_metric(api_session, port, key, ssh_tunnel, private_ip):
    """Return True when host's Mesos metric key is equal to value."""
    log.info('Polling metrics snapshot endpoint')
    auth_str = api_session.auth_user.auth_header['Authorization']
    curl_cmd = [
        'curl', '--insecure',
        '-H', 'Authorization:' + auth_str,
        api_session.default_url.scheme + '://' + private_ip + ':' + port + '/metrics/snapshot']
    response = json.loads(ssh_tunnel.command(curl_cmd).decode('utf-8'))
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


def get_upgrade_script_url(command_results: str):
    decoded_string = command_results.decode('utf-8')
    split_lines = decoded_string.splitlines()
    for line in split_lines:
        if "Node upgrade script URL: " in line:
            url_line = line
    log.info(url_line)
    split_in_words = url_line.split("Node upgrade script URL: ", 1)
    log.info(split_in_words)
    return split_in_words[1]


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

    with bootstrap_client.tunnel(bootstrap_host) as tunnel:
        platforms_onprem.download_dcos_installer(tunnel, installer_path, installer_url)
        log.info('Generating node upgrade script')
        tunnel.command(['sudo', 'bash', installer_path, '--help'])
        upgrade_version = json.loads(tunnel.command(
            ['sudo', 'bash', installer_path, '--version']).decode('utf-8'))['version']
        use_node_upgrade_script = not upgrade_version.startswith('1.8')
        if use_node_upgrade_script:
            tunnel_command = tunnel.command(
                ['sudo', 'bash', installer_path, '--generate-node-upgrade-script ' + starting_version]
            )
            upgrade_script_url = get_upgrade_script_url(tunnel_command)
        else:
            tunnel.command(['sudo', 'bash', installer_path, '--genconf'])
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
            with node_client.tunnel(host.public_ip) as tunnel:
                tunnel.command([
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
                    tunnel.command(['sudo', 'bash', 'dcos_node_upgrade.sh', '--verbose'], stdout=sys.stdout.buffer)
                else:
                    # If not using the dcoc-checks service, polling endpoints is
                    # required in order to pace the upgrade to persist state.
                    if use_node_upgrade_script:
                        if upgrade_version.startswith('1.1'):
                            # checks are implicit and must be disabled in 1.10 and above
                            tunnel.command(['sudo', 'bash', 'dcos_node_upgrade.sh', '--skip-checks', '--verbose'])
                        else:
                            # older installer have no concepts of checks
                            tunnel.command(['sudo', 'bash', 'dcos_node_upgrade.sh', '--verbose'])
                    else:
                        # no upgrade script to invoke, do upgrade manually
                        tunnel.command(['sudo', '-i', '/opt/mesosphere/bin/pkgpanda', 'uninstall'])
                        tunnel.command(['sudo', 'rm', '-rf', '/opt/mesosphere', '/etc/mesosphere'])
                        tunnel.command(['sudo', 'bash', 'dcos_install.sh', '-d', role])
                    log.info('Waiting for {} to rejoin the cluster...'.format(role_name))
                    if role == 'master':
                        port = "5050"
                        wait_key = 'registrar/log/recovered'
                    else:
                        port = "5051"
                        wait_key = 'slave/registered'
                    try:
                        wait_for_mesos_metric(dcos_api_session, port, wait_key, tunnel, host.private_ip)
                    except retrying.RetryError as exc:
                        raise Exception(
                            'Timed out waiting for {} to rejoin the cluster after upgrade: {}'.
                            format(role_name, repr(host.public_ip))
                        ) from exc
