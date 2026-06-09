# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import logging, os, sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from pjk.paths import logs_dir_path

logger = logging.getLogger("pjk")

def _truthy(v: Optional[str]) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")

def _level_from_env(explicit: Optional[int]) -> int:
    if explicit is not None:
        return explicit
    return logging.DEBUG if _truthy(os.getenv("DJK_DEBUG")) else logging.INFO

def _configure_logger(logger_obj, handlers, level: int) -> None:
    logger_obj.handlers.clear()
    for handler in handlers:
        handler.setLevel(level)
        logger_obj.addHandler(handler)
    logger_obj.setLevel(level)
    logger_obj.propagate = False

def init(force: bool = False, level: Optional[int] = None):
    """
    Initialize 'pjk' logging.

    - Rotates at DJK_LOG_MAX_MB (default 2 MB), keeps DJK_LOG_BACKUPS (default 3).
    - Files under PJK_HOME/logs by default; override with DJK_LOG_DIR / DJK_LOG_FILE.
    - Set DJK_DEBUG=1|true|yes for DEBUG, else INFO (or pass explicit level).
    - If the log directory is not writable, fall back to console logging
      (stderr → CloudWatch in AWS).
    - Set force=True to replace existing handlers.
    """
    if logger.handlers and not force:
        return
    logger.handlers.clear()

    level = _level_from_env(level)
    fmt = "[%(levelname)s] [%(threadName)s] %(message)s"
    formatter = logging.Formatter(fmt)

    try:
        # Preferred: rotating file handler under PJK_HOME/logs
        log_dir = Path(os.getenv("DJK_LOG_DIR", logs_dir_path()))
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / os.getenv("DJK_LOG_FILE", "pjk.log")
        max_bytes = int(float(os.getenv("DJK_LOG_MAX_MB", "2")) * 1024 * 1024)  # 2 MB
        backups = int(os.getenv("DJK_LOG_BACKUPS", "3"))

        fh = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backups,
            encoding="utf-8",
            delay=False,
        )
        fh.setFormatter(formatter)
        _configure_logger(logger, [fh], level)
    except Exception:
        # Fallback: console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        _configure_logger(logger, [ch], level)
        logger.warning("Falling back to console logging (log file not writable)")

def init_stream(force: bool = False, level: Optional[int] = None):
    """
    Console-only logging for PjkStream and other embedded/library use.

    Writes to stderr so AWS Lambda, ECS, and Fargate ship logs to CloudWatch
    without relying on PJK_HOME/logs or a writable home directory.
    """
    if logger.handlers and not force:
        return

    level = _level_from_env(level)
    fmt = "[%(levelname)s] [%(threadName)s] %(message)s"
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(fmt))
    _configure_logger(logger, [handler], level)
