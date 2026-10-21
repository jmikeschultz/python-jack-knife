# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

from unittest.mock import MagicMock, patch

import pytest
import yaml

from pjk.errors import RecordProcessingError
from pjk.integrations.rest_pipe import RestPipe, _build_url, _format_path
from pjk.usage import ParsedToken, Usage, configs


@pytest.fixture
def rest_config(monkeypatch, tmp_path):
    cfg = tmp_path / "configs.yaml"
    cfg.write_text(
        yaml.dump(
            {
                "RestPipe-demo": {
                    "base_url": "https://api.example.com",
                    "method": "POST",
                    "path": "/v1/items/{id}",
                    "auth": "bearer",
                    "token": "secret-token",
                    "timeout": 15,
                    "body": "record",
                },
                "RestPipe-subset": {
                    "base_url": "https://api.example.com/",
                    "method": "POST",
                    "path": "/v1/items/{id}",
                    "auth": "none",
                    "body": "record",
                    "body_fields": ["id", "amount"],
                },
                "RestPipe-oauth": {
                    "base_url": "https://api.example.com",
                    "method": "POST",
                    "path": "/v1/items/{id}",
                    "auth": "client_credentials",
                    "auth_url": "https://auth.example.com/token",
                    "auth_client_id": "my-client",
                    "auth_client_secret": "my-secret",
                    "body": "record",
                },
            }
        )
    )
    monkeypatch.setenv("PJK_CONFIG_FILE", str(cfg))
    # force reload
    configs._loaded_path = None
    configs._data = {}
    yield
    configs._loaded_path = None
    configs._data = {}


def _make_pipe(instance: str) -> RestPipe:
    token = f"rest:{instance}"
    ptok = ParsedToken(token)
    usage = RestPipe.usage()
    usage.bind(ptok)
    return RestPipe(ptok, usage)


def test_format_path_and_url():
    assert _format_path("/v1/items/{id}", {"id": "1"}) == "/v1/items/1"
    assert _build_url("https://api.example.com", "/v1/x") == "https://api.example.com/v1/x"
    assert _build_url("https://api.example.com/", "/v1/x") == "https://api.example.com/v1/x"


def test_format_path_missing_field():
    with pytest.raises(ValueError, match="missing path field"):
        _format_path("/v1/items/{id}", {})


def test_rest_post_yields_json_object(rest_config):
    pipe = _make_pipe("demo")
    pipe.add_source(iter([{"id": "1", "amount": 10}]))

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = {"ok": True, "id": "1"}
    mock_resp.text = '{"ok": true}'

    with patch("requests.request", return_value=mock_resp) as req:
        out = list(pipe)

    assert out == [{"ok": True, "id": "1"}]
    kwargs = req.call_args.kwargs
    assert kwargs["method"] == "POST"
    assert kwargs["url"] == "https://api.example.com/v1/items/1"
    assert kwargs["json"] == {"id": "1", "amount": 10}
    assert kwargs["headers"]["Authorization"] == "Bearer secret-token"
    assert kwargs["timeout"] == 15.0


def test_rest_body_fields_subset(rest_config):
    pipe = _make_pipe("subset")
    pipe.add_source(iter([{"id": "9", "amount": 5, "extra": "nope"}]))

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = {"ok": True}
    mock_resp.text = "{}"

    with patch("requests.request", return_value=mock_resp) as req:
        list(pipe)

    assert req.call_args.kwargs["json"] == {"id": "9", "amount": 5}


def test_rest_missing_path_field_record_error(rest_config):
    pipe = _make_pipe("demo")
    pipe.add_source(iter([{"amount": 10}]))

    with pytest.raises(RecordProcessingError) as ei:
        list(pipe)
    assert "missing path field" in str(ei.value.cause)
    assert ei.value.record["amount"] == 10


def test_rest_http_error_record_error(rest_config):
    pipe = _make_pipe("demo")
    pipe.add_source(iter([{"id": "1"}]))

    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_resp.text = "not found"
    mock_resp.headers = {}

    with patch("requests.request", return_value=mock_resp):
        with pytest.raises(RecordProcessingError) as ei:
            list(pipe)
    assert "HTTP 404" in str(ei.value.cause)


def test_rest_client_credentials_fetches_and_caches_token(rest_config):
    pipe = _make_pipe("oauth")
    pipe.add_source(iter([{"id": "1"}, {"id": "2"}]))

    token_resp = MagicMock()
    token_resp.status_code = 200
    token_resp.json.return_value = {"access_token": "tok-abc", "expires_in": 3600}
    token_resp.text = "{}"

    api_resp = MagicMock()
    api_resp.status_code = 200
    api_resp.headers = {"Content-Type": "application/json"}
    api_resp.json.return_value = {"ok": True}
    api_resp.text = "{}"

    with patch("requests.post", return_value=token_resp) as post:
        with patch("requests.request", return_value=api_resp) as req:
            out = list(pipe)

    assert out == [{"ok": True}, {"ok": True}]
    assert post.call_count == 1
    assert post.call_args.args[0] == "https://auth.example.com/token"
    assert post.call_args.kwargs["data"]["grant_type"] == "client_credentials"
    assert post.call_args.kwargs["data"]["client_id"] == "my-client"
    assert post.call_args.kwargs["data"]["client_secret"] == "my-secret"
    assert req.call_count == 2
    assert req.call_args_list[0].kwargs["headers"]["Authorization"] == "Bearer tok-abc"
    assert req.call_args_list[1].kwargs["headers"]["Authorization"] == "Bearer tok-abc"


def test_rest_client_credentials_refreshes_on_401(rest_config):
    pipe = _make_pipe("oauth")
    pipe.add_source(iter([{"id": "1"}]))

    token_resp_1 = MagicMock()
    token_resp_1.status_code = 200
    token_resp_1.json.return_value = {"access_token": "tok-old", "expires_in": 3600}
    token_resp_1.text = "{}"

    token_resp_2 = MagicMock()
    token_resp_2.status_code = 200
    token_resp_2.json.return_value = {"access_token": "tok-new", "expires_in": 3600}
    token_resp_2.text = "{}"

    unauthorized = MagicMock()
    unauthorized.status_code = 401
    unauthorized.text = "expired"
    unauthorized.headers = {}

    ok = MagicMock()
    ok.status_code = 200
    ok.headers = {"Content-Type": "application/json"}
    ok.json.return_value = {"ok": True}
    ok.text = "{}"

    with patch("requests.post", side_effect=[token_resp_1, token_resp_2]) as post:
        with patch("requests.request", side_effect=[unauthorized, ok]) as req:
            out = list(pipe)

    assert out == [{"ok": True}]
    assert post.call_count == 2
    assert req.call_count == 2
    assert req.call_args_list[0].kwargs["headers"]["Authorization"] == "Bearer tok-old"
    assert req.call_args_list[1].kwargs["headers"]["Authorization"] == "Bearer tok-new"


def test_rest_registered_as_integration():
    from pjk.common import Integration
    from pjk.pipes.factory import COMPONENTS

    assert COMPONENTS["rest"] is RestPipe
    assert issubclass(RestPipe, Integration)
