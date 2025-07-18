# TODO: move this to a separate module

import pyarrow as pa


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

    # create combined column names
    all_column_names = []
    all_columns = []
    all_names = set()
    for i, table in enumerate(tables):
        if overlap := set(table.column_names).intersection(all_names):
            raise ValueError(
                f"Duplicate column names {overlap} found when stacking table at index {i}: {table}"
            )
        all_names.update(table.column_names)
        all_column_names += table.column_names
        all_columns += table.columns

    return pa.Table.from_arrays(all_columns, names=all_column_names)


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
