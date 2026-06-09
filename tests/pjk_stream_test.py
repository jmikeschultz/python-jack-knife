# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import logging
import sys
from logging.handlers import RotatingFileHandler

import pytest
from pathlib import Path

from pjk import PjkStream
from pjk.history import write_history
from pjk.log import init_stream, logger
from pjk.runtime import enter_pjk_stream, exit_pjk_stream


def test_expression_selects_fields():
    records = list(
        PjkStream.expression("{foo:1,bar:2,baz:3} select:foo,bar -")
    )
    assert records == [{"foo": 1, "bar": 2}]


def test_tokens_equivalent_to_expression():
    expr_records = list(
        PjkStream.expression("[{foo:1},{foo:2}] select:foo -")
    )
    token_records = list(
        PjkStream.tokens(["[{foo:1},{foo:2}]", "select:foo", "-"])
    )
    assert expr_records == token_records == [{"foo": 1}, {"foo": 2}]


def test_inrecs_with_pipes_only():
    records = list(
        PjkStream.expression("select:foo,bar -", inrecs=[{"foo": 1, "bar": 2, "baz": 9}])
    )
    assert records == [{"foo": 1, "bar": 2}]


def test_file(tmp_path: Path):
    pjk_path = tmp_path / "pipe.pjk"
    pjk_path.write_text("select:foo,bar\n-\n", encoding="utf-8")
    records = list(
        PjkStream.file(pjk_path, inrecs=[{"foo": 10, "bar": 20, "extra": 99}])
    )
    assert records == [{"foo": 10, "bar": 20}]


def test_file_rejects_non_pjk_suffix():
    with pytest.raises(ValueError, match="expects a .pjk path"):
        PjkStream.file("/tmp/not-a-pjk-file")


def test_pjk_stream_never_writes_history(monkeypatch, tmp_path):
    monkeypatch.delenv("PJK_NO_HISTORY", raising=False)
    monkeypatch.chdir(tmp_path)
    list(PjkStream.expression("{foo:1} select:foo -"))
    assert not (tmp_path / ".pjk-history.txt").exists()


def test_write_history_blocked_while_stream_context_active(monkeypatch, tmp_path):
    monkeypatch.delenv("PJK_NO_HISTORY", raising=False)
    monkeypatch.chdir(tmp_path)
    token = enter_pjk_stream()
    try:
        write_history(["{foo:1}", "select:foo", "-"])
    finally:
        exit_pjk_stream(token)
    assert not (tmp_path / ".pjk-history.txt").exists()


def test_pjk_stream_uses_stderr_logging():
    init_stream(force=True)
    list(PjkStream.expression("{foo:1} select:foo -"))
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    assert isinstance(handler, logging.StreamHandler)
    assert not isinstance(handler, RotatingFileHandler)
    assert handler.stream is sys.stderr
