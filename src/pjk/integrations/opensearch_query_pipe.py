import os
import sys
import traceback
from copy import deepcopy
from typing import Optional, Iterator, Dict, Any, Iterable

from pjk.usage import ParsedToken, Usage
from pjk.pipes.query_pipe import QueryPipe
from pjk.common import Integration
from pjk.integrations.opensearch_client import OpenSearchClient, OS_CONFIG_TUPLES

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
    desc = ("Opensearch query pipe. Uses record['query'] or record['os_query_object']\n"
    "An instance may define 'default_index' otherwise the query object must include an 'index' field.\n")
    arg0 = ("instance", "instance to query over.")
    examples = [
        ["{'query': '_ping'}", 'os_query:myinst', '-'],
        ["{'index': 'myidx', 'query': '*'}", 'os_query:myinst', '-'],
        ["{'index': 'myidx', 'query': 'dog AND cat'}", 'os_query:myinst', '-'],
        ["{'index': 'myidx', 'query': 'dog'}", 'os_query:myinst', '-'],
        ["{'os_query_object': {query: {...}}", 'os_query:myinst', '-', ' # uses instance.default_index'],
    ]

     # name, type, default
    config_tuples = OS_CONFIG_TUPLES

    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)

        self.index = usage.get_config("default_index")
        self.client = OpenSearchClient.get_client(usage)

        # Iteration state
        self.cur_record: Optional[Dict[str, Any]] = None
        self.hits_iter: Optional[Iterator[Dict[str, Any]]] = None

    def reset(self):
        # keep the index open between drains
        pass

    def close(self):
        pass

    def ping(self):
        indexes = self.client.indices.get_alias(index="*")  
        index_list = []

        yield {'num_indexes': len(indexes.keys())}
        for index_name in sorted(indexes.keys()):
            try:
                count = self.client.count(index=index_name)["count"]
                yield {'index': index_name, 'count': count}

            except Exception as e:
                print(f"{index_name}: failed to count ({e})")

    def execute_query_returning_S_xO_iterable(self, query_record: dict) -> Iterator[Dict[str, Any]]:
        query_string = query_record.get('query', None)
        query_index = query_record.get('index', None)
        if query_index:
            self.index = query_index # overwrite the default query

        query_body = None

        if query_string:
            if query_string == '_ping':
                yield from self.ping()
                return

            query_body = build_body_from_string(query_string)
        else:
            query_body = query_record.get('os_query_object')

        if not query_body:
            yield {'_error': "query_record missing 'query' or 'os_query_object' field"}
            return

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
                "os_query_object": req_body
            }

            # Emit each hit
            for hit in hits:
                if "_source" in hit and isinstance(hit["_source"], dict):
                    yield hit["_source"]
                else:
                    # Some queries (e.g., stored fields only) might not include _source
                    yield {"_type": "os_query_hit", "_hit": hit}

        except Exception as e:
            #print("OpenSearch query error:", e, file=sys.stderr)
            #traceback.print_exc()
            yield {
                "_type": "os_query_error",
                "error": str(e),
                "query_record": query_record,
            }
