# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import pytest

from pjk.usage import Usage, ParsedToken, TokenError

def test_token_and_usage():

    usage = Usage("myop", "does cool stuff", None)
    usage.def_arg('weather', 'the weather outside')
    usage.def_param('color', 'the color of the thing')
    usage.def_param('flavor', 'the flavor of the thing')
              
    ptok = ParsedToken('myop:sunny@color=green')
    usage.bind(ptok)

    assert usage.get_arg('weather') == 'sunny'
    assert usage.get_param('color') == 'green'
    assert usage.get_param('flavor') == None

    ptok = ParsedToken('myop')
    try:
        usage.bind(ptok)
    except TokenError as e:
        message = e.__str__()
        print(message) # will only print with pytest -s


def test_boolean_param_bare_flag_means_true_when_default_false():
    usage = Usage("flagop", "flags", None)
    usage.def_param(
        "verbose",
        "more noise",
        valid_values={"true", "false"},
        default="false",
    )
    usage.def_param("note", "needs a value")

    ptok = ParsedToken("flagop@verbose")
    usage.bind(ptok)
    assert usage.get_param("verbose") == "true"

    usage2 = Usage("flagop2", "flags", None)
    usage2.def_param(
        "dry_run",
        "no write",
        valid_values={"true", "false"},
        default="true",
    )
    ptok2 = ParsedToken("flagop2@dry_run")
    with pytest.raises(TokenError):
        usage2.bind(ptok2)
