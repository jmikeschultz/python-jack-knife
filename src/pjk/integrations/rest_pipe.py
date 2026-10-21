# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

"""
Generic REST integration pipe (1 record in → 1 record out).

Configured via configs.yaml as RestPipe-<instance>. Path placeholders
``{field}`` are filled from the input record; JSON body is built from the
record (or a field subset). Response JSON object is yielded as the output
record.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

from pjk.common import Integration
from pjk.components import DeepCopyPipe
from pjk.errors import RecordProcessingError
from pjk.progress import papi
from pjk.usage import ParsedToken, Usage, configs

PATH_PARAM_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")

BODY_METHODS = frozenset({"POST", "PUT", "PATCH"})

# Refresh a bit before expires_in to avoid edge-of-expiry 401s.
_TOKEN_EXPIRY_SKEW_SEC = 30


def _format_path(path: str, record: dict) -> str:
    missing = []

    def repl(match: re.Match) -> str:
        name = match.group(1)
        if name not in record or record[name] is None:
            missing.append(name)
            return match.group(0)
        return str(record[name])

    out = PATH_PARAM_RE.sub(repl, path)
    if missing:
        raise ValueError(f"missing path field(s): {', '.join(missing)}")
    return out


def _build_url(base_url: str, path: str) -> str:
    base = base_url if base_url.endswith("/") else base_url + "/"
    rel = path.lstrip("/")
    return urljoin(base, rel)


class RestPipe(DeepCopyPipe, Integration):
    @classmethod
    def usage(cls):
        u = Usage(
            name="rest",
            desc=(
                "Generic REST integration: one HTTP request per input record.\n"
                "Yields one output record (JSON response body).\n"
                "Configure RestPipe-<instance> in configs.yaml with base_url, "
                "method, and path.\n"
                "Path placeholders {field} are filled from the input record."
            ),
            component_class=cls,
        )
        u.def_arg(
            name="instance",
            usage=(
                "configs.yaml entry RestPipe-<instance> must define "
                "base_url, method, path (and auth as needed)"
            ),
        )
        u.def_config_tuples(
            [
                ("base_url", str, None),
                ("method", str, None),
                ("path", str, None),
                ("auth", str, "none", True),  # none|bearer|basic|client_credentials
                ("token", str, None, True),
                ("token_env", str, None, True),
                ("username", str, None, True),
                ("password", str, None, True),
                ("auth_url", str, None, True),
                ("auth_client_id", str, None, True),
                ("auth_client_secret", str, None, True),
                ("timeout", float, 30.0, True),
                ("body", str, "record", True),  # record | none
            ]
        )
        u.def_example(
            expr_tokens=[
                "{id: '1', amount: 10}",
                "rest:demo",
            ],
            expect=None,
        )
        return u

    def __init__(self, ptok: ParsedToken, usage: Usage, root=None):
        super().__init__(ptok, usage, root)
        self.token = ptok.whole_token
        self.usage_obj = usage

        self.base_url = usage.get_config("base_url")
        self.method = (usage.get_config("method") or "").upper()
        self.path = usage.get_config("path")
        self.auth = (usage.get_config("auth") or "none").lower()
        self.token_value = usage.get_config("token")
        self.token_env = usage.get_config("token_env")
        self.username = usage.get_config("username")
        self.password = usage.get_config("password")
        self.auth_url = usage.get_config("auth_url")
        self.auth_client_id = usage.get_config("auth_client_id")
        self.auth_client_secret = usage.get_config("auth_client_secret")
        self.timeout = usage.get_config("timeout") or 30.0
        self.body_mode = (usage.get_config("body") or "record").lower()

        entry = self._resolved_entry()
        raw_fields = entry.get("body_fields") if entry else None
        if isinstance(raw_fields, list):
            self.body_fields: Optional[List[str]] = [str(x) for x in raw_fields]
        elif isinstance(raw_fields, str) and raw_fields.strip():
            self.body_fields = [p.strip() for p in raw_fields.split(",") if p.strip()]
        else:
            self.body_fields = None

        raw_headers = entry.get("headers") if entry else None
        self.extra_headers: Dict[str, str] = (
            {str(k): str(v) for k, v in raw_headers.items()}
            if isinstance(raw_headers, dict)
            else {}
        )

        if self.method not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}:
            raise ValueError(f"rest: unsupported method {self.method!r}")

        # Cached OAuth2 access token for auth=client_credentials
        self._access_token: Optional[str] = None
        self._access_token_expires_at: float = 0.0

        self.recs_in = papi.get_counter(self, "recs_in", display=False)
        self.recs_out = papi.get_percentage_counter(self, "recs_out", self.recs_in)

    def _resolved_entry(self) -> dict:
        configs._ensure_loaded()
        instance = self.usage_obj.get_arg("instance")
        key = f"{type(self).__name__}-{instance}"
        return configs._resolve_entry(key) or {}

    def reset(self):
        pass

    def close(self):
        pass

    def _fetch_client_credentials_token(self) -> Tuple[str, float]:
        """
        OAuth2 client_credentials against auth_url (OIDC/Keycloak-style).
        Returns (access_token, expires_at_epoch).
        """
        import requests

        if not self.auth_url:
            raise ValueError("rest auth=client_credentials requires config 'auth_url'")
        if not self.auth_client_id:
            raise ValueError("rest auth=client_credentials requires config 'auth_client_id'")
        if not self.auth_client_secret:
            raise ValueError(
                "rest auth=client_credentials requires config 'auth_client_secret'"
            )

        response = requests.post(
            self.auth_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.auth_client_id,
                "client_secret": self.auth_client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            snippet = (response.text or "")[:500]
            raise RuntimeError(
                f"HTTP {response.status_code} fetching token from {self.auth_url}: {snippet}"
            )

        payload = response.json()
        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError(
                f"token response from {self.auth_url} missing access_token"
            )
        expires_in = float(payload.get("expires_in") or 300)
        expires_at = time.time() + max(expires_in - _TOKEN_EXPIRY_SKEW_SEC, 1)
        return access_token, expires_at

    def _bearer_token(self) -> str:
        if self.auth == "bearer":
            token = self.token_value
            if not token and self.token_env:
                token = os.environ.get(self.token_env)
            if not token:
                raise ValueError(
                    "rest auth=bearer requires config 'token' or 'token_env' "
                    f"(env {self.token_env!r} empty/missing)"
                )
            return token

        if self.auth == "client_credentials":
            now = time.time()
            if self._access_token and now < self._access_token_expires_at:
                return self._access_token
            token, expires_at = self._fetch_client_credentials_token()
            self._access_token = token
            self._access_token_expires_at = expires_at
            return token

        raise ValueError(f"rest: no bearer token for auth={self.auth!r}")

    def _auth_headers_and_auth(self):
        import requests

        headers = dict(self.extra_headers)
        auth = None

        if self.auth in ("none", "", None):
            return headers, auth

        if self.auth in ("bearer", "client_credentials"):
            headers["Authorization"] = f"Bearer {self._bearer_token()}"
            return headers, auth

        if self.auth == "basic":
            if not self.username:
                raise ValueError("rest auth=basic requires config 'username'")
            auth = requests.auth.HTTPBasicAuth(self.username, self.password or "")
            return headers, auth

        raise ValueError(
            f"rest: unsupported auth {self.auth!r} "
            "(use none|bearer|basic|client_credentials)"
        )

    def _build_body(self, record: dict) -> Optional[dict]:
        if self.method not in BODY_METHODS:
            return None
        if self.body_mode == "none":
            return None
        if self.body_fields is not None:
            return {k: record[k] for k in self.body_fields if k in record}
        # body: record
        return dict(record)

    def _response_record(self, response) -> dict:
        content_type = response.headers.get("Content-Type", "") or ""
        text = response.text
        try:
            obj = response.json()
        except Exception:
            return {
                "status_code": response.status_code,
                "content_type": content_type.split(";")[0].strip(),
                "body": text,
            }

        if isinstance(obj, dict):
            return obj
        if isinstance(obj, list):
            return {"items": obj}
        return {"value": obj}

    def __iter__(self):
        import requests

        for record in self.left:
            self.recs_in.increment()
            try:
                path = _format_path(self.path, record)
                url = _build_url(self.base_url, path)
                headers, auth = self._auth_headers_and_auth()
                body = self._build_body(record)

                kwargs: Dict[str, Any] = {
                    "method": self.method,
                    "url": url,
                    "headers": headers,
                    "auth": auth,
                    "timeout": self.timeout,
                    "allow_redirects": True,
                }
                if body is not None:
                    kwargs["json"] = body

                response = requests.request(**kwargs)
                # One refresh+retry on 401 when using client_credentials.
                if (
                    response.status_code == 401
                    and self.auth == "client_credentials"
                ):
                    self._access_token = None
                    self._access_token_expires_at = 0.0
                    headers, auth = self._auth_headers_and_auth()
                    kwargs["headers"] = headers
                    kwargs["auth"] = auth
                    response = requests.request(**kwargs)

                if response.status_code >= 400:
                    snippet = (response.text or "")[:500]
                    raise RuntimeError(
                        f"HTTP {response.status_code} for {self.method} {url}: {snippet}"
                    )

                out = self._response_record(response)
            except RecordProcessingError:
                raise
            except Exception as e:
                raise RecordProcessingError(
                    token=self.token,
                    record=record if isinstance(record, dict) else {"value": record},
                    cause=e,
                ) from e

            self.recs_out.increment()
            yield out
