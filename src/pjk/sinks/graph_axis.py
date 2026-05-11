# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz


def apply_log_axes(ax, *, xlog: bool = False, ylog: bool = False) -> None:
    if xlog:
        ax.set_xscale("log")
    if ylog:
        ax.set_yscale("log")
