import os
import shutil
from pjk.main import execute_tokens

def test_local():
    DIR="/tmp/.pjk-tests"
    if os.path.isdir(DIR):
        shutil.rmtree(DIR)
    os.makedirs(DIR, exist_ok=True)

    execute_tokens(["[{up:1,size:3},{up:2,size:4}]", f"json:{DIR}/sinkdir"])
    execute_tokens([f"{DIR}/sinkdir", "expect:[{up:1,size:3},{up:2,size:4}]"])

    execute_tokens(["[{up:1,size:3},{up:2,size:4}]", f"{DIR}/test.json"])
    execute_tokens([f"{DIR}/test.json", "expect:[{up:1,size:3},{up:2,size:4}]"])

    execute_tokens(["[{up:1,size:3},{up:2,size:4}]", f"{DIR}/test.json.gz"])
    execute_tokens([f"{DIR}/test.json.gz", "expect:[{up:1,size:3},{up:2,size:4}]"])

    shutil.move(f"{DIR}/test.json", f"{DIR}/test.log")
    execute_tokens([f"{DIR}/test.log@format=json", "expect:[{up:1,size:3},{up:2,size:4}]"])

    shutil.move(f"{DIR}/test.json.gz", f"{DIR}/test.csv")
    execute_tokens([f"{DIR}/test.csv@format=json.gz", "expect:[{up:1,size:3},{up:2,size:4}]"])
