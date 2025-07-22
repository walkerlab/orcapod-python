# TODO: move this to a separate module

from collections import defaultdict
import pyarrow as pa
from collections.abc import Mapping, Collection
from typing import Any


def join_arrow_schemas(*schemas: pa.Schema) -> pa.Schema:
    """Join multiple Arrow schemas into a single schema, ensuring compatibility of fields. In particular,
    no field names should collide."""
    merged_fields = []
    for schema in schemas:
        merged_fields.extend(schema)
    return pa.schema(merged_fields)


def hstack_tables(*tables: pa.Table) -> pa.Table:
    """
    Horizontally stack multiple PyArrow tables by concatenating their columns.

    All input tables must have the same number of rows and unique column names.

    Args:
        *tables: Variable number of PyArrow tables to stack horizontally

    Returns:
        Combined PyArrow table with all columns from input tables

    Raises:
        ValueError: If no tables provided, tables have different row counts,
                   or duplicate column names are found
    """
    if len(tables) == 0:
        raise ValueError("At least one table is required for horizontal stacking.")
    if len(tables) == 1:
        return tables[0]

    N = len(tables[0])
    for table in tables[1:]:
        if len(table) != N:
            raise ValueError(
                "All tables must have the same number of rows for horizontal stacking."
            )

    # create combined schema
    all_fields = []
    all_names = set()
    for table in tables:
        for field in table.schema:
            if field.name in all_names:
                raise ValueError(
                    f"Duplicate column name '{field.name}' found in input tables."
                )
            all_fields.append(field)
            all_names.add(field.name)
    combined_schmea = pa.schema(all_fields)

    # create combined columns
    all_columns = []
    for table in tables:
        all_columns += table.columns

    return pa.Table.from_arrays(all_columns, schema=combined_schmea)


def check_arrow_schema_compatibility(
    incoming_schema: pa.Schema, target_schema: pa.Schema, strict: bool = False
) -> tuple[bool, list[str]]:
    # TODO: add strict comparison
    """
    Check if incoming schema is compatible with current schema.

    Args:
        incoming_schema: Schema to validate
        target_schema: Expected schema to match against
        strict: If True, requires exact match of field names and types. If False (default),
            incoming_schema can have additional fields or different types as long as they are compatible.

    Returns:
        Tuple of (is_compatible, list_of_errors)
    """
    errors = []

    # Create lookup dictionaries for efficient access
    incoming_fields = {field.name: field for field in incoming_schema}
    target_fields = {field.name: field for field in target_schema}

    # Check each field in target_schema
    for field_name, target_field in target_fields.items():
        if field_name not in incoming_fields:
            errors.append(f"Missing field '{field_name}' in incoming schema")
            continue

        incoming_field = incoming_fields[field_name]

        # Check data type compatibility
        if not target_field.type.equals(incoming_field.type):
            # TODO: if not strict, allow type coercion
            errors.append(
                f"Type mismatch for field '{field_name}': "
                f"expected {target_field.type}, got {incoming_field.type}"
            )

        # Check semantic_type metadata if present in current schema
        current_metadata = target_field.metadata or {}
        incoming_metadata = incoming_field.metadata or {}

        if b"semantic_type" in current_metadata:
            expected_semantic_type = current_metadata[b"semantic_type"]

            if b"semantic_type" not in incoming_metadata:
                errors.append(
                    f"Missing 'semantic_type' metadata for field '{field_name}'"
                )
            elif incoming_metadata[b"semantic_type"] != expected_semantic_type:
                errors.append(
                    f"Semantic type mismatch for field '{field_name}': "
                    f"expected {expected_semantic_type.decode()}, "
                    f"got {incoming_metadata[b'semantic_type'].decode()}"
                )
        elif b"semantic_type" in incoming_metadata:
            errors.append(
                f"Unexpected 'semantic_type' metadata for field '{field_name}': "
                f"{incoming_metadata[b'semantic_type'].decode()}"
            )

    # If strict mode, check for additional fields in incoming schema
    if strict:
        for field_name in incoming_fields:
            if field_name not in target_fields:
                errors.append(f"Unexpected field '{field_name}' in incoming schema")

    return len(errors) == 0, errors


def split_by_column_groups(
    table,
    *column_groups: Collection[str],
) -> tuple[pa.Table | None, ...]:
    """
    Split the table into multiple tables based on the provided column groups.
    Each group is a collection of column names that should be included in the same table.
    The remaining columns that are not part of any group will be returned as the first table/None.
    """
    if not column_groups:
        return (table,)

    tables = []
    remaining_columns = set(table.column_names)

    for group in column_groups:
        group_columns = [col for col in group if col in remaining_columns]
        if group_columns:
            tables.append(table.select(group_columns))
            remaining_columns.difference_update(group_columns)
        else:
            tables.append(None)

    remaining_table = None
    if remaining_columns:
        ordered_remaining_columns = [
            col for col in table.column_names if col in remaining_columns
        ]
        remaining_table = table.select(ordered_remaining_columns)
    return (remaining_table, *tables)


def prepare_prefixed_columns(
    table: pa.Table,
    prefix_info: Collection[str]
    | Mapping[str, Any | None]
    | Mapping[str, Mapping[str, Any | None]],
    exclude_columns: Collection[str] = (),
    exclude_prefixes: Collection[str] = (),
) -> tuple[pa.Table, dict[str, pa.Table]]:
    """ """
    all_prefix_info = {}
    if isinstance(prefix_info, Mapping):
        for prefix, info in prefix_info.items():
            if isinstance(info, Mapping):
                all_prefix_info[prefix] = info
            else:
                all_prefix_info[prefix] = info
    elif isinstance(prefix_info, Collection):
        for prefix in prefix_info:
            all_prefix_info[prefix] = {}
    else:
        raise TypeError(
            "prefix_group must be a Collection of strings or a Mapping of string to string or None."
        )

    # split column into prefix groups
    data_column_names = []
    data_columns = []
    existing_prefixed_columns = defaultdict(list)

    for col_name in table.column_names:
        prefix_found = False
        for prefix in all_prefix_info:
            if col_name.startswith(prefix):
                # Remove the prefix from the column name
                base_name = col_name.removeprefix(prefix)
                existing_prefixed_columns[prefix].append(base_name)
                prefix_found = True
        if not prefix_found:
            # if no prefix found, consider this as a data column
            data_column_names.append(col_name)
            data_columns.append(table[col_name])

    # Create source_info columns for each regular column
    num_rows = table.num_rows

    prefixed_column_names = defaultdict(list)
    prefixed_columns = defaultdict(list)

    target_column_names = [
        c
        for c in data_column_names
        if not any(c.startswith(prefix) for prefix in exclude_prefixes)
        and c not in exclude_columns
    ]

    for prefix, value_lut in all_prefix_info.items():
        target_prefixed_column_names = prefixed_column_names[prefix]
        target_prefixed_columns = prefixed_columns[prefix]

        for col_name in target_column_names:
            prefixed_col_name = f"{prefix}{col_name}"
            existing_columns = existing_prefixed_columns[prefix]

            if isinstance(value_lut, Mapping):
                value = value_lut.get(col_name)
            else:
                value = value_lut

            if value is not None:
                # Use value from source_info dictionary
                column_values = pa.array([value] * num_rows, type=pa.large_string())
            # if col_name is in existing_source_info, use that column
            elif col_name in existing_columns:
                # Use existing source_info column, but convert to large_string
                existing_col = table[prefixed_col_name]

                if existing_col.type == pa.string():
                    # Convert to large_string
                    column_values = pa.compute.cast(existing_col, pa.large_string())  # type: ignore
                else:
                    column_values = existing_col
            else:
                # Use null values
                column_values = pa.array([None] * num_rows, type=pa.large_string())
            target_prefixed_column_names.append(prefixed_col_name)
            target_prefixed_columns.append(column_values)

    # Step 3: Create the final table
    data_table: pa.Table = pa.Table.from_arrays(data_columns, names=data_column_names)
    result_tables = {}
    for prefix in all_prefix_info:
        result_tables[prefix] = pa.Table.from_arrays(
            prefixed_columns[prefix], names=prefixed_column_names[prefix]
        )
    return data_table, result_tables


def drop_schema_columns(schema: pa.Schema, columns: Collection[str]) -> pa.Schema:
    """
    Drop specified columns from a PyArrow schema.

    Args:
        schema (pa.Schema): The original schema.
        columns (list[str]): List of column names to drop.

    Returns:
        pa.Schema: New schema with specified columns removed.
    """
    return pa.schema([field for field in schema if field.name not in columns])
