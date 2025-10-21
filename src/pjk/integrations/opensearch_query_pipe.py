import os
import sys
import traceback
from copy import deepcopy
from typing import Optional, Iterator, Dict, Any, Iterable

from pjk.base import Pipe, ParsedToken, Usage, Integration
from pjk.pipes.query_pipe import QueryPipe
from pjk.common import Config
from pjk.integrations.opensearch_client import OpenSearchClient

def build_body_from_string(query_string: str) -> dict:
    if query_string == "*":
        return {"query": {"match_all": {}}}
    else:
        return {
            "query": {
                "simple_query_string": {
                    "query": query_string
                }
            }
        }

class OpenSearchQueryPipe(QueryPipe, Integration):
    name = "os_query"
    desc = "Opensearch query pipe. Uses record['query_string'] or record['query_object'] for os query"
    arg0 = ("index", "index to query over")
    examples = [
        ["{'query_string': '*'}", 'os_query:myindex', '-'],
        ["{'query_string': 'dog'}", 'os_query:myindex', '-'],
        ["{'query_string': 'dog AND cat'}", 'os_query:myindex', '-'],
        ["{'query_object': {query: {...}}", 'os_query:myindex', '-'],
    ]

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        # index from arg0 or config
        self.index = ptok.get_arg(0)

        # Build the OpenSearch client (handles AWS/basic/none)
        config = Config('index', self, self.index)
        self.client = OpenSearchClient.get_client(config)

        # Iteration state
        self.cur_record: Optional[Dict[str, Any]] = None
        self.hits_iter: Optional[Iterator[Dict[str, Any]]] = None

    def reset(self):
        # keep the index open between drains
        pass

    def close(self):
        pass

    def execute_query_returning_Q_xR_iterable(self, query_record: dict) -> Iterator[Dict[str, Any]]:
        query_string = query_record.get('query_string', None)
        query_body = None

        if query_string:
            query_body = build_body_from_string(query_string)
        else:
            query_body = query_record.get('query_object')

        try:
            # Build final request body
            req_body = deepcopy(query_body)
            req_body["size"] = self.count

            res = self.client.search(index=self.index, body=req_body)

            total_hits = 0
            took = res.get("took")
            hits = res.get("hits", {}).get("hits", [])
            total_obj = res.get("hits", {}).get("total", {})
            if isinstance(total_obj, dict):
                total_hits = total_obj.get("value", 0)
            elif isinstance(total_obj, int):
                total_hits = total_obj

            # Emit a metadata record first
            yield {
                "took_ms": took,
                "total_hits": total_hits,
                "index": self.index,
                "os_query_body": req_body
            }

            # Emit each hit
            for hit in hits:
                if "_source" in hit and isinstance(hit["_source"], dict):
                    yield hit["_source"]
                else:
                    # Some queries (e.g., stored fields only) might not include _source
                    yield {"_type": "os_query_hit", "_hit": hit}

        except Exception as e:
            print("OpenSearch query error:", e, file=sys.stderr)
            traceback.print_exc()
            yield {
                "_type": "os_query_error",
                "error": str(e),
                "query_record": query_record,
            }
