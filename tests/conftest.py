import pytest

@pytest.fixture(autouse=True)
def _disable_pjk_history(monkeypatch, tmp_path):
    # Option 1: completely disable history writes
    monkeypatch.setenv("PJK_NO_HISTORY", "1")

    # Option 2 (alternative): redirect to a temp file so nothing hits your real home
    # monkeypatch.setenv("PJK_HISTORY_FILE", str(tmp_path / ".pjk-history"))
