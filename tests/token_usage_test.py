# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from pjk.base import Usage, ParsedToken, TokenError

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
