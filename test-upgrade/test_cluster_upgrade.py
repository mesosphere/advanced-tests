from dcos_e2e.base_classes import ClusterBackend
from dcos_e2e.cluster import Cluster
from dcos_e2e.node import DCOSVariant, Output
from passlib.hash import sha512_crypt
import uuid


class TestUpgradeTests:
    """
    Tests for DC/OS upgrade.
    """

    def test_upgrade_from_url(
        self,
        docker_backend: ClusterBackend,
        ee_artifact_url: str,
        ee_upgrade_artifact_url: str,
        license_key_contents: str,
    ) -> None:
        """
        DC/OS EE can be upgraded from artifact_url to upgrade_artifact_url.
        """

        superuser_username = str(uuid.uuid4())
        superuser_password = str(uuid.uuid4())
        extra_config = {
            'superuser_username': superuser_username,
            'superuser_password_hash': sha512_crypt.hash(superuser_password),
            'security': 'strict',
            'fault_domain_enabled': False,
            'license_key_contents': license_key_contents,
        }
        with Cluster(cluster_backend=docker_backend) as cluster:
            cluster.install_dcos_from_url(
                dcos_installer=ee_artifact_url,
                dcos_config={
                    **cluster.base_config,
                    **extra_config,
                },
                output=Output.LOG_AND_CAPTURE,
                ip_detect_path=docker_backend.ip_detect_path,
            )
            cluster.wait_for_dcos_ee(
                superuser_username=superuser_username,
                superuser_password=superuser_password,
            )

            for node in {
                *cluster.masters,
                *cluster.agents,
                *cluster.public_agents,
            }:
                upgrade_from_ver = ee_artifact_url.split('/')[-2]
                maj, min, *_ = upgrade_from_ver.split('.')
                upgrade_to_ver = maj + '.' + str(int(min) + 1)

                build = node.dcos_build_info()
                assert build.version.startswith(upgrade_from_ver)
                assert build.variant == DCOSVariant.ENTERPRISE

            cluster.upgrade_dcos_from_url(
                dcos_installer=ee_upgrade_artifact_url,
                dcos_config={
                    **cluster.base_config,
                    **extra_config,
                },
                ip_detect_path=docker_backend.ip_detect_path,
                output=Output.LOG_AND_CAPTURE,
            )

            cluster.wait_for_dcos_ee(
                superuser_username=superuser_username,
                superuser_password=superuser_password,
            )
            for node in {
                *cluster.masters,
                *cluster.agents,
                *cluster.public_agents,
            }:
                build = node.dcos_build_info()
                assert build.version.startswith(upgrade_to_ver)
                assert build.variant == DCOSVariant.ENTERPRISE
