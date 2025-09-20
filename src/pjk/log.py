# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import logging, os, tempfile
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

logger = logging.getLogger("pjk")

def _truthy(v: Optional[str]) -> bool:
    return str(v).lower() in ("1", "true", "yes", "on")

def init(force: bool = False, level: Optional[int] = None):
    """
    Initialize 'pjk' logging.

    - Rotates at DJK_LOG_MAX_MB (default 2 MB), keeps DJK_LOG_BACKUPS (default 3).
    - Files under ~/.pjk/logs by default; override with DJK_LOG_DIR / DJK_LOG_FILE.
    - Set DJK_DEBUG=1|true|yes for DEBUG, else INFO (or pass explicit level).
    - If the log directory is not writable, fall back to console logging
      (stderr → CloudWatch in AWS).
    - Set force=True to replace existing handlers.
    """
    if logger.handlers and not force:
        return
    logger.handlers.clear()

    level = level or (logging.DEBUG if _truthy(os.getenv("DJK_DEBUG")) else logging.INFO)
    fmt = "[%(levelname)s] [%(threadName)s] %(message)s"
    formatter = logging.Formatter(fmt)

    try:
        # Preferred: rotating file handler under ~/.pjk/logs
        log_dir = Path(os.getenv("DJK_LOG_DIR", Path.home() / ".pjk" / "logs"))
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
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception:
        # Fallback: console handler
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.warning("Falling back to console logging (log file not writable)")

    logger.setLevel(level)
    # Do not propagate to root
    logger.propagate = False
