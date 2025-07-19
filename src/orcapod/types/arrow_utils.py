# from collections.abc import Mapping, Collection
# import pyarrow as pa
# from typing import Any


# def join_arrow_schemas(*schemas: pa.Schema) -> pa.Schema:
#     """Join multiple Arrow schemas into a single schema, ensuring compatibility of fields. In particular,
#     no field names should collide."""
#     merged_fields = []
#     for schema in schemas:
#         merged_fields.extend(schema)
#     return pa.schema(merged_fields)


# def split_by_column_groups(
#     self, *column_groups: Collection[str]
# ) -> tuple[pa.Table | None]:
#     """
#     Split the table into multiple tables based on the provided column groups.
#     Each group is a collection of column names that should be included in the same table.
#     The remaining columns that are not part of any group will be returned as the first table/None.
#     """
#     if not column_groups:
#         return (self,)

#     tables = []
#     remaining_columns = set(self.column_names)

#     for group in column_groups:
#         group_columns = [col for col in group if col in remaining_columns]
#         if group_columns:
#             tables.append(self.select(group_columns))
#             remaining_columns.difference_update(group_columns)
#         else:
#             tables.append(None)

#     remaining_table = None
#     if remaining_columns:
#         orderd_remaining_columns = self.column_names
#         remaining_columns = [
#             col for col in orderd_remaining_columns if col in remaining_columns
#         ]
#         remaining_table = self.select(orderd_remaining_columns)
#     return (remaining_table, *tables)


# def prepare_prefixed_columns(
#     table: pa.Table,
#     prefix_group: Collection[str] | Mapping[str, Any | None],
# ) -> tuple[pa.Table, pa.Table]:
#     """ """
#     if isinstance(prefix_group, Mapping):
#         prefix_group = {k: v if v is not None else {} for k, v in prefix_group.items()}
#     elif isinstance(prefix_group, Collection):
#         prefix_group = {name: {} for name in prefix_group}
#     else:
#         raise TypeError(
#             "prefix_group must be a Collection of strings or a Mapping of string to string or None."
#         )

#     # Visit each prefix group and split them into separate tables
#     member_columns = {}

#     for col_name in table.column_names:
#         for prefix in prefix_group:
#             if col_name.startswith(prefix):
#                 # Remove the prefix from the column name
#                 base_name = col_name.removeprefix(prefix)
#                 if base_name not in member_columns:
#                     member_columns[base_name] = []
#                 member_columns[base_name].append(table.column(col_name))

#     data_columns = []
#     data_column_names = []
#     existing_source_info = {}

#     for i, name in enumerate(table.column_names):
#         if name.startswith(SOURCE_INFO_PREFIX):
#             # Extract the base column name
#             base_name = name.removeprefix(SOURCE_INFO_PREFIX)
#             existing_source_info[base_name] = table.column(i)
#         else:
#             data_columns.append(table.column(i))
#             data_column_names.append(name)

#     # Step 2: Create source_info columns for each regular column
#     source_info_columns = []
#     source_info_column_names = []

#     # Create source_info columns for each regular column
#     num_rows = table.num_rows

#     for col_name in data_column_names:
#         source_info_col_name = f"{SOURCE_INFO_PREFIX}{col_name}"

#         # if col_name is in source_info, use that value
#         if col_name in source_info:
#             # Use value from source_info dictionary
#             source_value = source_info[col_name]
#             source_values = pa.array([source_value] * num_rows, type=pa.large_string())
#         # if col_name is in existing_source_info, use that column
#         elif col_name in existing_source_info:
#             # Use existing source_info column, but convert to large_string
#             existing_col = existing_source_info[col_name]
#             if existing_col.type == pa.large_string():
#                 source_values = existing_col
#             else:
#                 # Convert to large_string
#                 source_values = pa.compute.cast(existing_col, pa.large_string())  # type: ignore

#         else:
#             # Use null values
#             source_values = pa.array([None] * num_rows, type=pa.large_string())

#         source_info_columns.append(source_values)
#         source_info_column_names.append(source_info_col_name)

#     # Step 3: Create the final table
#     data_table: pa.Table = pa.Table.from_arrays(data_columns, names=data_column_names)
#     source_info_table: pa.Table = pa.Table.from_arrays(
#         source_info_columns, names=source_info_column_names
#     )
#     return data_table, source_info_table
