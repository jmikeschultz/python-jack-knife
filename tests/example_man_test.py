from pjk.main import execute_tokens

# these two test cover a lot basic functinonality because
# the working examples of the components cover a lot of the
# basic use cases.

# executes all man pages with working examples
def test_man_all():
    execute_tokens(["man","all+"])

# executes all working examples
def test_examples():
    execute_tokens(["examples+"])
