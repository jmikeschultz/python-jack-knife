# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import contextvars

_pjk_stream = contextvars.ContextVar("pjk_stream", default=False)


def pjk_stream_active() -> bool:
    return _pjk_stream.get()


def enter_pjk_stream():
    return _pjk_stream.set(True)


def exit_pjk_stream(token) -> None:
    _pjk_stream.reset(token)
