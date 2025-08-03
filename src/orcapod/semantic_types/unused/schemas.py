# from typing import Self
# from orcapod.types.core import DataType, TypeSpec
# from orcapod.types.semantic_types import (
#     SemanticType,
#     SemanticTypeRegistry,
#     PythonArrowConverter,
# )
# import pyarrow as pa
# import datetime

# # This mapping is expected to be stable
# # Be sure to test this assumption holds true
# DEFAULT_ARROW_TYPE_LUT = {
#     int: pa.int64(),
#     float: pa.float64(),
#     str: pa.large_string(),
#     bool: pa.bool_(),
# }


# def python_to_arrow_type(python_type: type) -> pa.DataType:
#     if python_type in DEFAULT_ARROW_TYPE_LUT:
#         return DEFAULT_ARROW_TYPE_LUT[python_type]
#     raise TypeError(f"Converstion of python type {python_type} is not supported yet")


# def arrow_to_python_type(arrow_type: pa.DataType) -> type:
#     if pa.types.is_integer(arrow_type):
#         return int
#     elif pa.types.is_floating(arrow_type):
#         return float
#     elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
#         return str
#     elif pa.types.is_boolean(arrow_type):
#         return bool
#     elif pa.types.is_date(arrow_type):
#         return datetime.date
#     elif pa.types.is_timestamp(arrow_type):
#         return datetime.datetime
#     elif pa.types.is_binary(arrow_type):
#         return bytes
#     else:
#         raise TypeError(f"Conversion of arrow type {arrow_type} is not supported")


# class PythonSchema(dict[str, DataType]):
#     """
#     A schema for Python data types, mapping string keys to Python types.

#     This is used to define the expected structure of data packets in OrcaPod.

#     Attributes
#     ----------
#     keys : str
#         The keys of the schema.
#     values : type
#         The types corresponding to each key.

#     Examples
#     --------
#     >>> schema = PythonSchema(name=str, age=int)
#     >>> print(schema)
#     {'name': <class 'str'>, 'age': <class 'int'>}
#     """

#     def copy(self) -> "PythonSchema":
#         return PythonSchema(self)

#     def to_semantic_schema(
#         self, semantic_type_registry: SemanticTypeRegistry
#     ) -> "SemanticSchema":
#         """
#         Convert the Python schema to a semantic schema using the provided semantic type registry.

#         Parameters
#         ----------
#         semantic_type_registry : SemanticTypeRegistry
#             The registry containing semantic type information.

#         Returns
#         -------
#         SemanticSchema
#             A new schema mapping keys to tuples of Python types and optional semantic type identifiers.

#         Examples
#         --------
#         >>> python_schema = PythonSchema(name=str, age=int)
#         >>> semantic_schema = python_schema.to_semantic_schema(registry)
#         >>> print(semantic_schema)
#         {'name': (str, None), 'age': (int, None)}
#         """
#         return SemanticSchema.from_typespec(self, semantic_type_registry)

#     def to_arrow_schema(
#         self,
#         semantic_type_registry: SemanticTypeRegistry | None = None,
#         converters: dict[str, PythonArrowConverter] | None = None,
#     ) -> pa.Schema:
#         """
#         Convert the Python schema to an Arrow schema.
#         If converters are provided, they are used to convert the schema. Note that
#         no validation is performed on the converters, so they must be compatible with the schema.
#         """
#         if converters is not None:
#             # If converters are provided, use them to convert the schema
#             fields = []
#             for field_name, python_type in self.items():
#                 if field_name in converters:
#                     converter = converters[field_name]
#                     arrow_type = converter.arrow_type
#                     metadata = None
#                     if converter.semantic_type_name is not None:
#                         metadata = {
#                             b"semantic_type": converter.semantic_type_name.encode(
#                                 "utf-8"
#                             )
#                         }
#                 else:
#                     arrow_type = python_to_arrow_type(python_type)
#                     metadata = None
#                 fields.append(pa.field(field_name, arrow_type, metadata=metadata))
#             return pa.schema(fields)

#         if semantic_type_registry is None:
#             raise ValueError(
#                 "semantic_type_registry must be provided if converters are not"
#             )
#         # Otherwise, convert using the semantic type registry
#         return self.to_semantic_schema(semantic_type_registry).to_arrow_schema()

#     @classmethod
#     def from_semantic_schema(cls, semantic_schema: "SemanticSchema") -> Self:
#         """
#         Create a PythonSchema from a SemanticSchema.

#         Parameters
#         ----------
#         semantic_schema : SemanticSchema
#             The semantic schema to convert.

#         Returns
#         -------
#         PythonSchema
#             A new schema mapping keys to Python types.
#         """
#         return cls(semantic_schema.get_python_types())

#     @classmethod
#     def from_arrow_schema(
#         cls,
#         arrow_schema: pa.Schema,
#         semantic_type_registry: SemanticTypeRegistry | None = None,
#         converters: dict[str, PythonArrowConverter] | None = None,
#     ) -> Self:
#         """
#         Create a PythonSchema from an Arrow schema.

#         Parameters
#         ----------
#         arrow_schema : pa.Schema
#             The Arrow schema to convert.
#         semantic_type_registry : SemanticTypeRegistry
#             The registry containing semantic type information.
#         skip_system_columns : bool, optional
#             Whether to skip system columns (default is True).
#         converters : dict[str, PythonArrowConverter], optional
#             A dictionary of converters to use for converting the schema. If provided, the schema will be
#             converted using the converters. If not provided, the schema will be converted using the semantic type
#             registry.

#         Returns
#         -------
#         PythonSchema
#             A new schema mapping keys to Python types.
#         """
#         if converters is not None:
#             # If converters are provided, use them to convert the schema
#             python_types = {}
#             for field in arrow_schema:
#                 # TODO: consider performing validation of semantic type
#                 if field.name in converters:
#                     converter = converters[field.name]
#                     python_types[field.name] = converter.python_type
#                 else:
#                     python_types[field.name] = arrow_to_python_type(field.type)
#             return cls(python_types)

#         if semantic_type_registry is None:
#             raise ValueError(
#                 "semantic_type_registry must be provided if converters are not"
#             )
#         semantic_schema = SemanticSchema.from_arrow_schema(
#             arrow_schema,
#             semantic_type_registry,
#         )
#         return cls(semantic_schema.get_python_types())


# class SemanticSchema(dict[str, type | SemanticType]):
#     """
#     A schema for semantic types, mapping string keys to tuples of Python types and optional metadata.

#     This is used to define the expected structure of data packets with semantic types in OrcaPod.

#     Attributes
#     ----------
#     keys : str
#         The keys of the schema.
#     values : type | SemanticType
#         Either type for simple fields or SemanticType for semantic fields.

#     Examples
#     --------
#     >>> schema = SemanticSchema(image=SemanticType('path'), age=int)
#     >>> print(schema)
#     {"image": SemanticType(name='path'), "age": <class 'int'>})}
#     """

#     def get_semantic_fields(self) -> dict[str, SemanticType]:
#         """
#         Get a dictionary of semantic fields in the schema.

#         Returns
#         -------
#         dict[str, SemanticType]
#             A dictionary mapping keys to their corresponding SemanticType.
#         """
#         return {k: v for k, v in self.items() if isinstance(v, SemanticType)}

#     def get_python_types(self) -> dict[str, type]:
#         """
#         Get the Python types for all keys in the schema.

#         Returns
#         -------
#         dict[str, type]
#             A dictionary mapping keys to their corresponding Python types.
#         """
#         return {
#             k: v.get_default_python_type() if isinstance(v, SemanticType) else v
#             for k, v in self.items()
#         }

#     def get_arrow_types(self) -> dict[str, tuple[pa.DataType, str | None]]:
#         """
#         Get the Arrow types for all keys in the schema.

#         Returns
#         -------
#         dict[str, tuple[pa.DataType, str|None]]
#             A dictionary mapping keys to tuples of Arrow types. If the field has a semantic type,
#             the second element of the tuple is the semantic type name; otherwise, it is None.
#         """
#         return {
#             k: (v.get_default_arrow_type(), v.name)
#             if isinstance(v, SemanticType)
#             else (python_to_arrow_type(v), None)
#             for k, v in self.items()
#         }

#     def to_arrow_schema(self) -> pa.Schema:
#         """
#         Get the Arrow schema, which is a PythonSchema representation of the semantic schema.

#         Returns
#         -------
#         PythonSchema
#             A new schema mapping keys to Python types.
#         """
#         fields = []
#         for k, (arrow_type, semantic_type_name) in self.get_arrow_types().items():
#             if semantic_type_name is not None:
#                 field = pa.field(
#                     k,
#                     arrow_type,
#                     metadata={b"semantic_type": semantic_type_name.encode("utf-8")},
#                 )
#             else:
#                 field = pa.field(k, arrow_type)
#             fields.append(field)

#         return pa.schema(fields)

#     def to_python_schema(self) -> PythonSchema:
#         """
#         Get the Python schema, which is a PythonSchema representation of the semantic schema.

#         Returns
#         -------
#         PythonSchema
#             A new schema mapping keys to Python types.
#         """
#         return PythonSchema.from_semantic_schema(self)

#     @classmethod
#     def from_arrow_schema(
#         cls,
#         arrow_schema: pa.Schema,
#         semantic_type_registry: SemanticTypeRegistry,
#     ) -> Self:
#         """
#         Create a SemanticSchema from an Arrow schema.

#         Parameters
#         ----------
#         arrow_schema : pa.Schema
#             The Arrow schema to convert.

#         Returns
#         -------
#         SemanticSchema
#             A new schema mapping keys to tuples of Python types and optional semantic type identifiers.
#         """

#         semantic_schema = {}
#         for field in arrow_schema:
#             field_type = None
#             if field.metadata is not None:
#                 semantic_type_name = field.metadata.get(b"semantic_type", b"").decode()
#                 if semantic_type_name:
#                     semantic_type = semantic_type_registry.get_semantic_type(
#                         semantic_type_name
#                     )
#                     if semantic_type is None:
#                         raise ValueError(
#                             f"Semantic type '{semantic_type_name}' not found in registry"
#                         )
#                     if not semantic_type.supports_arrow_type(field.type):
#                         raise ValueError(
#                             f"Semantic type '{semantic_type.name}' does not support Arrow field of type '{field.type}'"
#                         )
#                     field_type = semantic_type

#             if (
#                 field_type is None
#             ):  # was not set to semantic type, so fallback to simple conversion
#                 field_type = arrow_to_python_type(field.type)

#             semantic_schema[field.name] = field_type
#         return cls(semantic_schema)

#     @classmethod
#     def from_typespec(
#         cls,
#         typespec: TypeSpec,
#         semantic_type_registry: SemanticTypeRegistry,
#     ) -> Self:
#         semantic_schema = {}
#         for key, python_type in typespec.items():
#             semantic_type = semantic_type_registry.get_semantic_type_for_python_type(
#                 python_type
#             )
#             if semantic_type is not None:
#                 semantic_schema[key] = semantic_type
#             else:
#                 semantic_schema[key] = python_type
#         return cls(semantic_schema)
