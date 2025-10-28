# djk/sinks/opensearch_sink.py

from pjk.components import Sink
from pjk.common import Integration
from pjk.usage import ParsedToken, Usage, CONFIG_FILE
from pjk.integrations.opensearch_client import OpenSearchClient, OS_CONFIG_TUPLES
from opensearchpy import helpers
from datetime import datetime

examples = [
    ["index_docs.json", 'let:index:myidx', 'os_index:myinst'],
    ["{'_command': 'create', 'config': {settings: {...}, mappings: {...}}, 'index': 'myidx'}", 'os_index:myinst'],
    ["{'_command': 'delete_index', 'index': 'myidx'}", "os_index:myinst"]
]

class OpenSearchIndexSink(Sink, Integration):
    @classmethod
    def usage(cls):
        usage = Usage(
            name="os_index",
            component_class=cls,
            desc="Opensearch indexer sink. All input records must contain 'index' field.\nOptional param 'id_field' can be specified."
        )
        usage.def_arg("instance", f"Instance in {CONFIG_FILE} to index into.")
        usage.def_param('id_field', usage='field to be used as unique id')
        usage.def_param('DDHHMM_key', usage='All commands require key equal to current day, hour, minute', is_num=True, default="0")
        usage.def_config_tuples(OS_CONFIG_TUPLES)
        for e in examples:
            usage.def_example(e, None)
        return usage
    
    def __init__(self, ptok: ParsedToken, usage: Usage):
        super().__init__(ptok, usage)
        self.client = OpenSearchClient.get_client(usage)
        self.bulk_size = 500
        self.buffer = []
        self.total_written = 0
        self.id_field = usage.get_param('id_field')
        self.ddhhmm_key = usage.get_param('DDHHMM_key')

    def execute_command(self, command: str, index: str, record: dict):
        current_ddhhmm = int(datetime.now().strftime("%d%H%M"))
        if self.ddhhmm_key != current_ddhhmm:
            raise Exception('All os commands require the DDHHMM_key corresponding to now, e.g. 031431')

        if command == 'create':
            config = record.pop('config', None)
            if not config:
                raise Exception('create command missing config object.')
            self.client.indices.create(index=index, body=config)

        elif command == 'delete_index':
            self.client.indices.delete(index=index)

        else:
            raise Exception(f'unknown os command: {command}')

    def process(self):
        for record in self.input:
            index = record.pop('index', None)
            if not index:
                raise Exception("All index records must contain an 'index' field")

            # Create index if config present
            command = record.pop('_command', None)
            if command:
                self.execute_command(command, index, record)
                continue

            action = {
                "_op_type": "index",     # use "create" to fail if doc already exists
                "_index": index,
                "_source": record        # meta fields removed; store only the actual doc
            }

            doc_id = record.get(self.id_field, None)
            if doc_id:
                action['_id'] = doc_id

            self.buffer.append(action)

            if len(self.buffer) >= self.bulk_size:
                self.flush()

        self.flush()

    def flush(self):
        if self.buffer:
            helpers.bulk(self.client, self.buffer)
            self.total_written += len(self.buffer)
            self.buffer.clear()

    def deep_copy(self):
        return None  # until deep sync available
