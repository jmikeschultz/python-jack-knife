import subprocess
from pathlib import Path

def test_user_source_with_user_sink():
    fixtures = Path(__file__).parent / "fixtures"
    source = fixtures / "user_source.py"
    sink = fixtures / "user_sink.py"

    result = subprocess.run(
        ["pjk", str(source), str(sink)],
        capture_output=True,
        text=True,
    )

    assert "hello" in result.stdout
    assert "world" in result.stdout
