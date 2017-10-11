import unittest
from configparser import ConfigParser

import os

from rc_support import _generate_config, _gen_and_init_env


class TestRcSupport(unittest.TestCase):

    def test_generate_config__no_overlap_with_env(self):
        env = {}
        config = ConfigParser()
        config['x'] = {
            'a': "1",
            'b': "2"
        }

        expected = {
            "a": "1",
            "b": "2"
        }

        self.assertEqual(expected, _generate_config("x", env, config))

    def test_generate_config__does_not_override_defined_values_in_env(self):
        env = {
            "a": "should not be overridden"
        }
        config = ConfigParser()
        config['x'] = {
            'a': "1",
            'b': "2"
        }

        expected = {
            "a": "should not be overridden",
            "b": "2"
        }

        self.assertEqual(expected, _generate_config("x", env, config))

    def test_load_config_is_case_sensitive(self):
        expected1 = {
            "x": "1",
            "X": "one"
        }
        expected2 = {
            "x": "err_lower",
            "X": "err_upper"
        }

        config_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_resources", "rc_support",
                                  "test_load_config_is_case_sensitive")
        actual1 = _gen_and_init_env("a", {}, config_dir)
        self.assertEqual(expected1, actual1)
        actual2 = _gen_and_init_env("A", {}, config_dir)
        self.assertEqual(expected2, actual2)

    def test_load_file_and_init_env(self):
        expected = {
            "TEST_CREATE_CLUSTER": "true",
            "TEST_CUSTOM_CHECKS": "true",
            "TEST_INSTALL_PREREQS": "false"
        }

        config_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_resources", "rc_support",
                                  "test_load_file_and_init_env")
        actual = _gen_and_init_env("installer-cli", {}, config_dir)
        self.assertEqual(expected, actual)

    def test_file_does_not_exist(self):

        config_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_resources", "rc_support",
                                  "test_should_never_exist")

        actual = _gen_and_init_env("x", {}, config_dir)
        self.assertEqual({}, actual)
