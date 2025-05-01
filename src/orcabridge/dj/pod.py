
from .stream import QueryStream
from ..name import pascal_to_snake, snake_to_pascal
from ..hash import hash_dict
from ..pod import FunctionPod, FunctionPodWithDirStorage
from ..types import PodFunction
from .mapper import JoinQuery
from ..operation import Operation
import datajoint as dj
from datajoint import Schema
from typing import Collection, Optional

import logging

logger = logging.getLogger(__name__)


class TableWrapper(Operation):
    def __init__(self, fp: FunctionPod, schema: Schema, table_name: str = None, streams: Collection[QueryStream] = None):
        self.fp = fp
        self.schema = schema
        self.table_name = table_name if table_name is not None else pascal_to_snake(fp.function.__name__)
        self.streams = streams if streams is not None else []
        self.table = None

    def compile(self) -> None:
        if not all(isinstance(s, QueryStream) for s in self.streams):
            raise ValueError("All streams must be QueryStreams")
        
        # assign all upstrem tables to a local list
        upstream_tables = [table for stream in self.streams for table in stream.upstream_tables]
        #upstreams = '\n'.join([f"-> self.streams[{i}].upstream_tables[{j}]" for i, stream in enumerate(self.streams) for j in range(len(stream.upstream_tables))])
        upstreams = "\n".join(f"-> upstream_tables[{i}]" for i in range(len(upstream_tables)))
        outputs = '\n'.join([f"{k}: varchar(255)" for k in self.fp.output_keys])

        class Table(dj.Computed):
            definition = f"""
            # {self.table_name} outputs
            {upstreams}
            ---
            {outputs}
            """

            def make(self, key):
                

        Table.__name__ = snake_to_pascal(self.table_name)
        Table = self.schema(Table)
        self.table = Table()


    def forward(self, *args):
        pass


    

# class FunctionPodWithTableStore(FunctionPod):
#     def __init__(self, function, output_keys, schema, table_name=None, )

# class DJFunctionPod(FunctionPod):
#     def __init__(self, function, output_keys, schema, name=None, store="./"):
#         self.name = name if name is not None else function.__name__
#         # store function name as snake_case
#         self.name = pascal_to_snake(self.name)
#         self.uuid_field = f"{self.name}_uuid"
#         self.function = function
#         self.output_keys = output_keys
#         self.schema = schema
#         self.store_dir = store
#         self.table = self.prepare_table()

#     def prepare_table(self):
#         class Table(dj.Manual):
#             definition = f"""
#             # {self.name} outputs
#             {self.uuid_field}: uuid  # UUID for the function pod entry
#             ---
#             input_files: varchar(2055) # map to input paths
#             {{outputs}}
#             """.format(outputs='\n'.join([f"{k}: varchar(255)" for k in self.output_keys]))
#         Table.__name__ = snake_to_pascal(self.name)
#         Table = self.schema(Table)
#         return Table()
    
    
#     def memoize(self, tag, element, output):
#         # create a key for the table
#         key = {self.uuid_field: hash_dict(element)}
#         # verify the key doesn't exist
#         if not self.table & key:
#             # insert the new entry into the table
#             input_files = ','.join([f"{k}:{v}" for k, v in element.items()])
#             self.table.insert1({**key, **output, 'input_files': input_files})
#             return True
#         else:
#             logger.info(f"Key {key} already exists in the table")
#             return False
    
#     def retrieve_memoized(self, element):
#         key = {self.uuid_field: hash_dict(element)}
#         if self.table & key:
#             # if the key already exists, return the output
#             row = (self.table & key).fetch1()
#             output = {k: row[k] for k in self.output_keys}
#             return True, output
#         return False, None

#     def execute(self, *streams):
#         return sum(1 for _ in self(*streams))
            
#     def __iter__(self):
#         # iterate over the table
#         return iter(FixedStreamFromTable(self.table))
    


class DJFunctionPod(FunctionPod):
    """
    Specialized FunctionPod that can only operate on DataJoint table-based streams.
    As it works on table-based streams, it automatically creates tables with proper foreign keys
    mapping out the execution path
    """
    def __init__(self, function, output_keys, schema, table_name, function_name=None, store="./pod_data", force_multi_source=False):
        self.schema = schema
        self.fp = FunctionPodWithDirStorage(function, output_keys, store_name=function_name, store_dir=store)
        self.force_multi_source = force_multi_source
        self.table_name = snake_to_pascal(table_name)
        self.upstreams = {}
        self.table = None
        self._table_stream = None

    @property
    def tables(self):
        return self._table_stream.tables

    @property
    def query(self):
        return self._table_stream.query

    def __call__(self, *streams: QueryStream) -> QueryStream:
        # verify that all stream are DJStreams
        if len(streams) < 1:
            raise ValueError("DJFunctionPod requires at least one stream")

        if not all(isinstance(s, QueryStream) for s in streams):
            raise ValueError("All streams must be QueryStreams")
        if len(streams) > 1:
            joined_streams = JoinQuery(*streams)
        else:
            joined_streams = streams[0]

        self.update_upstreams(joined_streams.tables)

        return self.fp(joined_streams)

    def update_upstreams(self, tables):
        sorted_tables = sorted(tables)
        key = ",".join(sorted_tables)
        self.upstreams[key] = sorted_tables

    def compile(self):
        """
        Compile the function pod into a DataJoint pipeline based on execution history
        """
        # create a table for each upstream
        data_table = None
        print("upstreams are", self.upstreams)
        if len(self.upstreams) > 0 or self.force_multi_source:
            table_classes = []
            part_tables = []
            field_prefix = pascal_to_snake(self.table_name)
            for idx, (key, tables) in enumerate(self.upstreams.items()):
                table_classes.append([dj.FreeTable(dj.conn(), t) for t in tables])
                upstreams = "\n".join(
                    [
                        f"-> table_classes[{idx}][{i}]"
                        for i in range(len(table_classes[idx]))
                    ]
                )
                print("upstreams are", upstreams)
                outer_self = self

                # create a table for each upstream
                class Table(dj.Part, dj.Computed):
                    definition = f"""
                    # {self.fp.name} outputs
                    -> master
                    ---
                    {upstreams}
                    """

                    @property
                    def key_source(self):
                        upstreams = self.parents(primary=False, as_objects=True)
                        source = upstreams[0].proj()
                        for table in upstreams[1:]:
                            source = source * table.proj()
                        return source

                    def make(self, key, _skip_computation=True):
                        print("Working on ", key)
                        upstreams = self.parents(primary=False, as_objects=True)
                        source = upstreams[0]
                        for table in upstreams[1:]:
                            source = source * table
                        print(source)
                        # get only non primary keys
                        secondary_keys = source.heading.secondary_attributes
                        element = (source & key).fetch(*secondary_keys, as_dict=True)
                        assert (
                            len(element) == 1
                        ), f"Expected one element, got {len(element)}"
                        element = element[0]

                        print(f"Fetched element: {element}")

                        entry = outer_self.fp.table & {
                            outer_self.fp.uuid_field: hash_dict(element)
                        }
                        if entry:
                            # hashing is done using MySQL table name
                            master_key = {
                                f"{field_prefix}_source_uuid": hash_dict(
                                    dict(key, part_table=self.table_name)
                                )
                            }
                            master_key[f"{field_prefix}_part_table"] = (
                                self.__class__.__name__
                            )
                            key.update(master_key)

                            outputs = entry.fetch(
                                *outer_self.fp.output_keys, as_dict=True
                            )
                            assert (
                                len(outputs) == 1
                            ), f"Expected one output, got {len(outputs)}"
                            master_key.update(outputs[0])

                            print("Master key: ", master_key)
                            print("Key: ", key)
                            self.master.insert1(master_key)
                            self.insert1(key)
                            print(f"Inserted key: {key}")
                        else:
                            print(f"No entry found for key: {key}")

                Table.__name__ = f"Source{idx}"
                part_tables.append(Table)

            outputs = "\n".join([f"{k}: varchar(255)" for k in self.fp.output_keys])

            class MasterTable(dj.Manual):
                definition = f"""
                {field_prefix}_source_uuid: uuid
                {field_prefix}_part_table: varchar(255)
                ---
                {outputs}
                """

            MasterTable.__name__ = self.table_name
            for table in part_tables:
                setattr(MasterTable, table.__name__, table)

            data_table = self.schema(MasterTable)
            print("data table is", data_table)
            self.table = data_table

        self._table_stream = FixedStreamFromTable(self.table)

        return self.table

    def __iter__(self):
        # iterate over the table
        if self.table is None:
            self.compile()
        yield from self._table_stream
