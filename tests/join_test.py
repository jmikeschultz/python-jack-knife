from pjk.main import execute_tokens

def test_concat():
    execute_tokens([
                "[{up:1}, {up:2}, {up:3}]", # source 1
                "[{down:4}, {down:5}, {down:6}]",
                "join:concat",
                "-"])