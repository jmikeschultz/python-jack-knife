# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import logging, os

logger = logging.getLogger("djk")

def init():
    if logger.hasHandlers(): return
    level = logging.DEBUG if os.getenv("DJK_DEBUG") in ("1", "true", "yes") else logging.INFO
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] [%(threadName)s] %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(level)
    logger.addHandler(handler)
