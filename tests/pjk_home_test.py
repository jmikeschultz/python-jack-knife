# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import pytest

from pjk.paths import (
    PJK_CONFIG_FILE_ENV,
    PJK_HOME_ENV,
    PJK_MACROS_FILE_ENV,
    PJK_PLUGINS_DIR_ENV,
    config_file_path,
    logs_dir_path,
    macros_file_path,
    plugins_dir_path,
    pjk_home,
)
from pjk.usage import Config


@pytest.fixture(autouse=True)
def _clear_pjk_env(monkeypatch):
    for key in (
        PJK_HOME_ENV,
        PJK_CONFIG_FILE_ENV,
        PJK_MACROS_FILE_ENV,
        PJK_PLUGINS_DIR_ENV,
    ):
        monkeypatch.delenv(key, raising=False)


def test_pjk_home_defaults_to_dot_pjk():
    from pathlib import Path

    assert pjk_home() == Path.home() / ".pjk"


def test_pjk_home_env_override(monkeypatch, tmp_path):
    home = tmp_path / "deploy-pjk"
    monkeypatch.setenv(PJK_HOME_ENV, str(home))
    assert pjk_home() == home.resolve()
    assert config_file_path() == home / "configs.yaml"
    assert macros_file_path() == home / "macros.txt"
    assert plugins_dir_path() == home / "plugins"
    assert logs_dir_path() == home / "logs"


def test_pjk_config_file_override(monkeypatch, tmp_path):
    config = tmp_path / "custom-configs.yaml"
    monkeypatch.setenv(PJK_CONFIG_FILE_ENV, str(config))
    assert config_file_path() == config.resolve()


def test_config_loads_from_pjk_home(monkeypatch, tmp_path):
    from tests.config_extends_test import ConfigComponent, _lookup

    home = tmp_path / "pjk_home"
    home.mkdir()
    (home / "configs.yaml").write_text(
        """
ConfigComponent-deploy:
   host: lambda-host
   port: 443
"""
    )
    monkeypatch.setenv(PJK_HOME_ENV, str(home))

    cfg = Config()
    assert _lookup(cfg, "deploy", "host") == "lambda-host"
    assert _lookup(cfg, "deploy", "port") == 443
