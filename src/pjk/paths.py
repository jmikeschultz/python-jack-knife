# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import os
from pathlib import Path

PJK_HOME_ENV = "PJK_HOME"
PJK_CONFIG_FILE_ENV = "PJK_CONFIG_FILE"
PJK_MACROS_FILE_ENV = "PJK_MACROS_FILE"
PJK_PLUGINS_DIR_ENV = "PJK_PLUGINS_DIR"


def pjk_home() -> Path:
    """
    Root directory for pjk runtime files.

    PJK_HOME if set, otherwise ~/.pjk (dev default).
    """
    override = os.environ.get(PJK_HOME_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".pjk"


def config_file_path() -> Path:
    """Component instance configs (OpenSearchQueryPipe-products, etc.)."""
    override = os.environ.get(PJK_CONFIG_FILE_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return pjk_home() / "configs.yaml"


def macros_file_path() -> Path:
    override = os.environ.get(PJK_MACROS_FILE_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return pjk_home() / "macros.txt"


def plugins_dir_path() -> Path:
    override = os.environ.get(PJK_PLUGINS_DIR_ENV)
    if override:
        return Path(override).expanduser().resolve()
    return pjk_home() / "plugins"


def logs_dir_path() -> Path:
    return pjk_home() / "logs"


def config_file_display() -> str:
    """Human-readable config path for errors and man pages."""
    return str(config_file_path())
