# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import pytest

from pjk.usage import Config, TokenError, Usage


class ConfigComponent:
    @classmethod
    def usage(cls):
        usage = Usage("cfgop", "config test component", cls)
        usage.def_arg("instance", "config instance name")
        usage.def_config_tuples(
            [
                ("host", str, None),
                ("port", int, 9200),
                ("default_index", str, None, True),
            ]
        )
        return usage


def _make_config(yaml_text: str) -> Config:
    from pjk.paths import config_file_path

    cfg = Config()
    cfg._data = __import__("yaml").safe_load(yaml_text) or {}
    cfg._loaded_path = str(config_file_path())
    return cfg


def _lookup(cfg: Config, instance: str, param: str):
    usage = ConfigComponent.usage()
    usage.args["instance"] = instance
    return cfg.lookup(usage, param)


def test_extends_inherits_base_params():
    cfg = _make_config(
        """
ConfigComponent-base:
   host: query-host
   port: 9200
   default_index: products-read

ConfigComponent-derived:
   _extends: ConfigComponent-base
"""
    )
    assert _lookup(cfg, "derived", "host") == "query-host"
    assert _lookup(cfg, "derived", "port") == 9200
    assert _lookup(cfg, "derived", "default_index") == "products-read"


def test_extends_local_values_override_base():
    cfg = _make_config(
        """
ConfigComponent-base:
   host: query-host
   port: 9200
   default_index: products-read

ConfigComponent-derived:
   _extends: ConfigComponent-base
   default_index: products-write
"""
    )
    assert _lookup(cfg, "derived", "host") == "query-host"
    assert _lookup(cfg, "derived", "default_index") == "products-write"


def test_extends_uses_tuple_default_when_param_not_in_yaml():
    cfg = _make_config(
        """
ConfigComponent-base:
   host: query-host

ConfigComponent-derived:
   _extends: ConfigComponent-base
"""
    )
    assert _lookup(cfg, "derived", "port") == 9200


def test_extends_missing_param_raises_with_extends_hint():
    cfg = _make_config(
        """
ConfigComponent-base:
   port: 9200

ConfigComponent-derived:
   _extends: ConfigComponent-base
"""
    )
    with pytest.raises(TokenError, match=r"host.*missing.*ConfigComponent-derived.*_extends: 'ConfigComponent-base'"):
        _lookup(cfg, "derived", "host")


def test_extends_missing_base_raises():
    cfg = _make_config(
        """
ConfigComponent-derived:
   _extends: ConfigComponent-missing
"""
    )
    with pytest.raises(TokenError, match="non-existent entry: 'ConfigComponent-missing'"):
        _lookup(cfg, "derived", "host")


def test_extends_cycle_raises():
    cfg = _make_config(
        """
ConfigComponent-a:
   _extends: ConfigComponent-b
   host: a

ConfigComponent-b:
   _extends: ConfigComponent-a
   host: b
"""
    )
    with pytest.raises(TokenError, match="Cycle in .* config inheritance"):
        _lookup(cfg, "a", "host")
