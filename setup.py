# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

# setup.py
from setuptools import setup, find_packages

setup(
    name="djk",
    version="0.1",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "pjk = djk.main:main",
        ],
    },
)
