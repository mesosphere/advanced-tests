# advanced-tests (OS Upgrade Testing)
This repo is used for automated DC/OS upgrade testing.

Essential Documents are:
*  [Running the Upgrade Qualification Tests](https://docs.google.com/document/d/1T9kfvGPbxuc8Gq31iI42QOAuoKDQblxZ5ILvvUhPooo/edit#)
* [Ducumented Upgrade Qualification Testruns](https://docs.google.com/spreadsheets/d/1zW3UsMq8Myw2GSG7t_JdJHkjLniz8TBJPhXRaOmKtZ0/edit)
* [Upgrade Qualification Chart (Matrix)](https://docs.google.com/spreadsheets/d/13YucIODQOvRVlkd8VBh6sUuCelj5HMQ0Yu46sLVhWDQ/edit#gid=356377462) which is mainly internally used
* [Team-City Job](https://teamcity.mesosphere.io/viewType.html?buildTypeId=DcOs_Enterprise_Test_UpgradeTest_WipFromEarlierStableMinorVersion&branch_DcOs_Enterprise_Test_UpgradeTest=%3Cdefault%3E&tab=buildTypeStatusDiv) to kickoff automated upgrade testing

Entry points are:
* `test_aws_cf_failure.py`: launchs applications, kills all agents, and waits for the application to return to normal operation
* `test_installer_cli.py`: runs through an installation using the CLI commands of `dcos_generate_config.sh` AKA the onprem installer
* `test_upgrade.py`: runs the upgrade procedure against a cluster

## Dependencies
* [dcos-launch](http://github.com/dcos/dcos-launch)
* [dcos-test-utils](http://github.com/mesosphere/dcos-test-utils)

## Running Tests with tox
By default, only flake8 is run with tox. Individual tests can be triggered by supplying the `-e` option to `tox` or by directly invoking pytest from your own development environment.
