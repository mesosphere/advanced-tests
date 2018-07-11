# advanced-tests
This repo is intended for tests that require modifying a DC/OS cluster beyond the supported APIs.
* `test_aws_cf_failure.py`: launchs applications, kills all agents, and waits for the application to return to normal operation
* `test_installer_cli.py`: runs through an installation using the CLI commands of `dcos_generate_config.sh` AKA the onprem installer
* `test_upgrade.py`: runs the upgrade procedure against a cluster

## Dependencies
* [dcos-launch](http://github.com/dcos/dcos-launch)
* [dcos-test-utils](http://github.com/mesosphere/dcos-test-utils)

## Running Tests with tox
By default, only flake8 is run with tox. Individual tests can be triggered by supplying the `-e` option to `tox` or by directly invoking pytest from your own development environment.
