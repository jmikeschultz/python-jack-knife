import shlex
import shutil
import subprocess

import pytest

from pjk.history import get_history_tokens, printable_command, read_history, write_history
from pjk.main import execute_tokens


@pytest.mark.parametrize("tokens", [
    ['{"color":"red","label":"red food"}', 'where:f.color == "red"', '-'],
    ["{color:'red'}", "where:f.color == 'red'", '-'],
    ['{text:"it\'s red"}', 'where:f.text == "it\'s red"', '-'],
    ['input file.json', 'where:f.size > 1', 'output file.json'],
    ['{text:"pjk hello"}', 'let:value="a # b"', '-'],
    ['{text:"$HOME `echo hello` $(echo hello)"}', 'let:path="C:\\tmp"', '-'],
    ['source', '', '#literal', '-'],
])
def test_history_round_trip_preserves_arguments(monkeypatch, tmp_path, tokens):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PJK_NO_HISTORY", raising=False)

    command = printable_command(tokens)
    assert shlex.split(command, comments=True, posix=True) == tokens
    write_history(tokens)
    write_history(tokens)

    assert read_history('.pjk-history.txt')[0] == [command]
    assert get_history_tokens('+1') == tokens


def test_replayed_double_quoted_expression_executes(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PJK_NO_HISTORY", raising=False)
    tokens = [
        '[{"color":"red"},{"color":"blue"}]',
        'where:f.color == "red"',
        'expect:[{"color":"red"}]',
    ]
    write_history(tokens)
    execute_tokens(get_history_tokens('+1'))


def test_legacy_command_prefix_is_still_supported(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / '.pjk-history.txt').write_text('pjk "{color: red}" -\n')
    assert get_history_tokens('+1') == ['{color: red}', '-']


def test_single_quoted_sql_uses_readable_double_quotes():
    tokens = ["{query: 'select * from campaigns'}", 'postgres:reporting', 'devnull']
    assert printable_command(tokens) == (
        '"{query: \'select * from campaigns\'}" postgres:reporting devnull'
    )


@pytest.mark.parametrize('shell', ['sh', 'zsh'])
@pytest.mark.parametrize('token', [
    "{query: 'select * from campaigns'}",
    '{text: "it\'s red"}',
    "let:path='C:\\tmp\\file'",
    "let:text='$HOME'",
    "let:text='$(printf expanded)'",
    "let:text='`printf expanded`'",
    "let:text='hello!world'",
    "'",
    "",
])
def test_readable_quotes_preserve_arguments_in_shlex_and_real_shells(token, shell):
    executable = shutil.which(shell)
    if executable is None:
        pytest.skip(f'{shell} is not installed')
    tokens = [token, 'devnull']
    command = printable_command(tokens)
    assert shlex.split(command, comments=True, posix=True) == tokens
    result = subprocess.run(
        [executable, '-c', "printf '%s\\0' " + command],
        capture_output=True, check=True,
    )
    assert result.stdout.decode().split('\0')[:-1] == tokens
