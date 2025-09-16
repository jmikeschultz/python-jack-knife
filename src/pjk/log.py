# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import logging, os, atexit
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

logger = logging.getLogger("djk")

def _truthy(env_val: Optional[str]) -> bool:
    return str(env_val).lower() in ("1", "true", "yes", "on")

def init(force: bool = False, level: Optional[int] = None, console: Optional[bool] = None):
    """
    Initialize 'djk' logging.

    - Rotates at DJK_LOG_MAX_MB (default 2 MB), keeps DJK_LOG_BACKUPS (default 3).
    - Files under ~/.pjk/logs by default; override with DJK_LOG_DIR / DJK_LOG_FILE.
    - Set DJK_DEBUG=1|true|yes for DEBUG, else INFO (or pass explicit level).
    - To enable console output explicitly, set console=True or DJK_LOG_CONSOLE=1.
    - Set force=True to replace existing handlers.
    """
    if logger.handlers and not force:
        return

    logger.handlers.clear()

    if level is None:
        level = logging.DEBUG if _truthy(os.getenv("DJK_DEBUG")) else logging.INFO

    fmt = "[%(levelname)s] [%(threadName)s] %(message)s"
    formatter = logging.Formatter(fmt)

    # Rotating file handler in ~/.pjk/logs
    log_dir = Path(os.getenv("DJK_LOG_DIR", os.path.expanduser("~/.pjk/logs")))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / os.getenv("DJK_LOG_FILE", "pjk.log")
    max_bytes = int(float(os.getenv("DJK_LOG_MAX_MB", "2")) * 1024 * 1024)  # 2 MB
    backups = int(os.getenv("DJK_LOG_BACKUPS", "3"))

    fh = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backups,
        encoding="utf-8",
        delay=False,  # open immediately so first emit writes bytes
    )
    fh.setLevel(level)
    fh.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(fh)

    # DO NOT propagate into root (prevents accidental console logs elsewhere)
    logger.propagate = False

    # Optional console (off by default)
    enable_console = console if console is not None else _truthy(os.getenv("DJK_LOG_CONSOLE"))
    if enable_console:
        sh = logging.StreamHandler()
        sh.setLevel(level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    # Flush/close on exit for short-lived runs
    atexit.register(logging.shutdown)
