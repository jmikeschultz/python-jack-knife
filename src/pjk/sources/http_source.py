# SPDX-License-Identifier: Apache-2.0
# Copyright 2024 Mike Schultz

import json
from typing import Any, Dict

import requests

from pjk.components import Source
from pjk.usage import ParsedToken, TokenError, Usage


class HttpUsage(Usage):
    """Binds URL from post_colon so http:https://host/path works (colon-split args would break URLs)."""

    def __init__(self, component_class: type):
        super().__init__(
            name="http",
            desc="http source. JSON responses yield records like json source;",
            component_class=component_class,
        )
        self.def_syntax("http:<url> | http://... | https://...")
        self.def_param(
            "timeout", "request timeout in seconds", is_num=True, default="30"
        )
        self.def_param(
            "method",
            "HTTP method",
            is_num=False,
            valid_values={"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"},
            default="GET",
        )

    def bind(self, ptok: ParsedToken):
        scheme = ptok.pre_colon.lower()
        rest = ptok.post_colon.strip()
        if scheme not in ('http', 'https') or not rest:
            raise TokenError.from_list(
                [
                    "missing url — use http:https://example.com/path, http://example.com, or https://example.com",
                    "",
                    self.get_usage_text(),
                ]
            )
        if rest.startswith('//'):
            url = f'{scheme}:{rest}'
        elif rest.startswith(('http://', 'https://')):
            url = rest
        else:
            url = f'{scheme}://{rest}'
        self.args['url'] = url
        self.bind_params(ptok)


class HttpSource(Source):
    def __init__(self, ptok: ParsedToken, usage: HttpUsage):
        super().__init__(root=None)
        self._url = usage.get_arg("url")
        self._timeout = usage.get_param("timeout")
        self._method = usage.get_param("method")
        self._usage = usage

    @classmethod
    def usage(cls):
        return HttpUsage(cls)

    def __iter__(self):
        try:
            r = requests.request(
                self._method, self._url, timeout=self._timeout, allow_redirects=True
            )
            r.raise_for_status()
        except requests.RequestException as e:
            raise TokenError.from_list(
                [f"http request failed: {e}", "", self._usage.get_usage_text()]
            ) from e

        text = r.text
        content_type = r.headers.get("Content-Type", "") or ""
        ctype_main = content_type.split(";")[0].strip()

        try:
            obj: Any = json.loads(text)
        except json.JSONDecodeError:
            rec: Dict[str, Any] = {
                "url": r.url,
                "status_code": r.status_code,
                "content_type": ctype_main,
                "body": text,
            }
            yield rec
            return

        if isinstance(obj, list):
            for item in obj:
                yield item
        else:
            yield obj
