import pyarrow as pa


def join_arrow_schemas(*schemas: pa.Schema) -> pa.Schema:
    """Join multiple Arrow schemas into a single schema, ensuring compatibility of fields. In particular,
    no field names should collide."""
    merged_fields = []
    for schema in schemas:
        merged_fields.extend(schema)
    return pa.schema(merged_fields)
