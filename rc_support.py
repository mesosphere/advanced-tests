import copy

from configparser import ConfigParser
from typing import Dict

import os


def init_runtime_config(namespace: str):
    os.environ = _gen_and_init_env(namespace, copy.copy(os.environ), os.getcwd())


def _gen_and_init_env(namespace: str, env: Dict[str, str], config_dir: str) -> Dict[str, str]:
    f_name = os.path.join(config_dir, ".advancedtestsrc")
    c = _load_config(f_name)
    new_env = _generate_config(namespace, env, c)
    return new_env


def _load_config(file_path: str) -> ConfigParser:
    cp = ConfigParser()
    cp.optionxform = lambda option: option
    try:
        with open(file_path, 'r') as f:
            cp.read_file(f)
    except FileNotFoundError:
        # intentionally swallow this error, if the file is not defined that's okay we will just not load it
        pass
    return cp


def _generate_config(namespace: str, env: Dict[str, str], config: ConfigParser) -> Dict[str, str]:
    """
    Generate a new enriched `env` with values defined in `config[namespace]` that do not already exist in `env`.

    :param namespace: The namespace in the `config` that should be used
    :param env:       The starting environment we want to add values from `config[namespace]` to
    :param config:    The config to read from
    :return:          The new `env` enriched with values from `config[namespace]`
    """
    if namespace in config:
        values = config[namespace]
        new_env = env.copy()
        for k, v in values.items():
            if k not in env:
                new_env[k] = v

        return new_env
    else:
        return env
