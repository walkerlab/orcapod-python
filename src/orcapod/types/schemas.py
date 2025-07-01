
from orcapod.types import  TypeSpec
from orcapod.types.semantic_type_registry import SemanticTypeRegistry
from typing import Any
import pyarrow as pa
import datetime

# This mapping is expected to be stable
# Be sure to test this assumption holds true
DEFAULT_ARROW_TYPE_LUT = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.large_string(),
    bool: pa.bool_(),
}

def python_to_arrow_type(python_type: type) -> pa.DataType:
    if python_type in DEFAULT_ARROW_TYPE_LUT:
        return DEFAULT_ARROW_TYPE_LUT[python_type]
    raise TypeError(f"Converstion of python type {python_type} is not supported yet")

def arrow_to_python_type(arrow_type: pa.DataType) -> type:
    if pa.types.is_integer(arrow_type):
        return int
    elif pa.types.is_floating(arrow_type):
        return float
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return str
    elif pa.types.is_boolean(arrow_type):
        return bool
    elif pa.types.is_date(arrow_type):
        return datetime.date
    elif pa.types.is_timestamp(arrow_type):
        return datetime.datetime
    elif pa.types.is_binary(arrow_type):
        return bytes
    else:
        raise TypeError(f"Conversion of arrow type {arrow_type} is not supported")



class PythonSchema(dict[str, type]):
    """
    A schema for Python data types, mapping string keys to Python types.
    
    This is used to define the expected structure of data packets in OrcaPod.
    
    Attributes
    ----------
    keys : str
        The keys of the schema.
    values : type
        The types corresponding to each key.
    
    Examples
    --------
    >>> schema = PythonSchema(name=str, age=int)
    >>> print(schema)
    {'name': <class 'str'>, 'age': <class 'int'>}
    """
    @property
    def with_source_info(self) -> dict[str, type]:
        """
        Get the schema with source info fields included.
        
        Returns
        -------
        dict[str, type|None]
            A new schema including source info fields.
        """
        return {**self, **{f'_source_info_{k}': str for k in self.keys()}}



class SemanticSchema(dict[str, tuple[type, str|None]]):
    """
    A schema for semantic types, mapping string keys to tuples of Python types and optional metadata.
    
    This is used to define the expected structure of data packets with semantic types in OrcaPod.
    
    Attributes
    ----------
    keys : str
        The keys of the schema.
    values : tuple[type, str|None]
        The types and optional semantic type corresponding to each key.
    
    Examples
    --------
    >>> schema = SemanticSchema(image=(str, 'path'), age=(int, None))
    >>> print(schema)
    {'image': (<class 'str'>, 'path'), 'age': (<class 'int'>, None)}
    """
    def get_store_type(self, key: str) -> type | None:
        """
        Get the storage type for a given key in the schema.
        
        Parameters
        ----------
        key : str
            The key for which to retrieve the storage type.
        
        Returns
        -------
        type | None
            The storage type associated with the key, or None if not found.
        """
        return self.get(key, (None, None))[0]

    def get_semantic_type(self, key: str) -> str | None:
        """
        Get the semantic type for a given key in the schema.
        
        Parameters
        ----------
        key : str
            The key for which to retrieve the semantic type.
        
        Returns
        -------
        str | None
            The semantic type associated with the key, or None if not found.
        """
        return self.get(key, (None, None))[1]
    
    @property
    def storage_schema(self) -> PythonSchema:
        """
        Get the storage schema, which is a PythonSchema representation of the semantic schema.
        
        Returns
        -------
        PythonSchema
            A new schema mapping keys to Python types.
        """
        return PythonSchema({k: v[0] for k, v in self.items()})

    
    @property
    def storage_schema_with_source_info(self) -> dict[str, type]:
        """
        Get the storage schema with source info fields included.
        
        Returns
        -------
        dict[str, type]
            A new schema including source info fields.
        
        Examples
        --------
        >>> semantic_schema = SemanticSchema(name=(str, 'name'), age=(int, None))
        >>> storage_schema = semantic_schema.storage_schema_with_source_info
        >>> print(storage_schema)
        {'name': <class 'str'>, 'age': <class 'int'>, '_source_info_name': <class 'str'>, '_source_info_age': <class 'str'>}
        """
        return self.storage_schema.with_source_info


def from_typespec_to_semantic_schema(
    typespec: TypeSpec,
    semantic_type_registry: SemanticTypeRegistry,
) -> SemanticSchema:
    """
    Convert a Python schema to a semantic schema using the provided semantic type registry.
    
    Parameters
    ----------
    typespec : TypeSpec
        The typespec to convert, mapping keys to Python types.
    semantic_type_registry : SemanticTypeRegistry
        The registry containing semantic type information.
    
    Returns
    -------
    SemanticSchema
        A new schema mapping keys to tuples of Python types and optional semantic type identifiers.
    
    Examples
    --------
    >>> typespec: TypeSpec = dict(name=str, age=int)
    >>> semantic_schema = from_typespec_to_semanticn_schema(typespec, registry)
    >>> print(semantic_schema)
    {'name': (<class 'str'>, None), 'age': (<class 'int'>, None)}
    """
    semantic_schema = {}
    for key, python_type in typespec.items():
        if python_type in semantic_type_registry:
            type_info = semantic_type_registry.get_type_info(python_type)
            assert type_info is not None, f"Type {python_type} should be found in the registry as `in` returned True"
            semantic_schema[key] = (type_info.storage_type, type_info.semantic_type)
        else:
            semantic_schema[key] = (python_type, None)
    return SemanticSchema(semantic_schema)

def from_semantic_schema_to_python_schema(
    semantic_schema: SemanticSchema,
    semantic_type_registry: SemanticTypeRegistry,
) -> PythonSchema:
    """
    Convert a semantic schema to a Python schema using the provided semantic type registry.
    
    Parameters
    ----------
    semantic_schema : SemanticSchema
        The schema to convert, mapping keys to tuples of Python types and optional semantic type identifiers.
    semantic_type_registry : SemanticTypeRegistry
        The registry containing semantic type information.
    
    Returns
    -------
    PythonSchema
        A new schema mapping keys to Python types.
    
    Examples
    --------
    >>> semantic_schema = SemanticSchema(name=(str, None), age=(int, None))
    >>> python_schema = from_semantic_schema_to_python_schema(semantic_schema, registry)
    >>> print(python_schema)
    {'name': <class 'str'>, 'age': <class 'int'>}
    """
    python_schema_content = {}
    for key, (python_type, semantic_type) in semantic_schema.items():
        if semantic_type is not None:
            # If the semantic type is registered, use the corresponding Python type
            python_type = semantic_type_registry.get_python_type(semantic_type)
        python_schema_content[key] = python_type
    return PythonSchema(python_schema_content)

def from_semantic_schema_to_arrow_schema(
    semantic_schema: SemanticSchema,
    include_source_info: bool = True,
) -> pa.Schema:
    """
    Convert a semantic schema to an Arrow schema.
    
    Parameters
    ----------
    semantic_schema : SemanticSchema
        The schema to convert, mapping keys to tuples of Python types and optional semantic type identifiers.
    
    Returns
    -------
    dict[str, type]
        A new schema mapping keys to Arrow-compatible types.
    
    Examples
    --------
    >>> semantic_schema = SemanticSchema(name=(str, None), age=(int, None))
    >>> arrow_schema = from_semantic_schema_to_arrow_schema(semantic_schema)
    >>> print(arrow_schema)
    {'name': str, 'age': int}
    """
    fields = []
    for field_name, (python_type, semantic_type) in semantic_schema.items():
        arrow_type = DEFAULT_ARROW_TYPE_LUT[python_type]
        field_metadata = {b"semantic_type": semantic_type.encode('utf-8')} if semantic_type else {}
        fields.append(pa.field(field_name, arrow_type, metadata=field_metadata))

    if include_source_info:
        for field in semantic_schema:
            field_metadata = {b'field_type': b'source_info'}
            fields.append(pa.field(f'_source_info_{field}', pa.large_string(), metadata=field_metadata))
        
    return pa.schema(fields)

def from_arrow_schema_to_semantic_schema(
    arrow_schema: pa.Schema,
) -> SemanticSchema:
    """
    Convert an Arrow schema to a semantic schema.
    
    Parameters
    ----------
    arrow_schema : pa.Schema
        The schema to convert, containing fields with metadata.
    
    Returns
    -------
    SemanticSchema
        A new schema mapping keys to tuples of Python types and optional semantic type identifiers.
    
    Examples
    --------
    >>> arrow_schema = pa.schema([pa.field('name', pa.string(), metadata={'semantic_type': 'name'}),
    ...                           pa.field('age', pa.int64(), metadata={'semantic_type': 'age'})])
    >>> semantic_schema = from_arrow_schema_to_semantic_schema(arrow_schema)
    >>> print(semantic_schema)
    {'name': (str, 'name'), 'age': (int, 'age')}
    """
    semantic_schema = {}
    for field in arrow_schema:
        if field.metadata.get(b'field_type', b'') == b'source_info':
            # Skip source info fields
            continue
        semantic_type = field.metadata.get(b'semantic_type', None)
        semantic_type = semantic_type.decode() if semantic_type else None
        python_type = arrow_to_python_type(field.type)
        semantic_schema[field.name] = (python_type, semantic_type)
    return SemanticSchema(semantic_schema)

def from_typespec_to_arrow_schema(typespec: TypeSpec,
    semantic_type_registry: SemanticTypeRegistry, include_source_info: bool = True) -> pa.Schema:
    semantic_schema = from_typespec_to_semantic_schema(typespec, semantic_type_registry)
    return from_semantic_schema_to_arrow_schema(semantic_schema, include_source_info=include_source_info)


def from_arrow_schema_to_python_schema(
    arrow_schema: pa.Schema,
    semantic_type_registry: SemanticTypeRegistry,
) -> PythonSchema:
    """
    Convert an Arrow schema to a Python schema.
    
    Parameters
    ----------
    arrow_schema : pa.Schema
        The schema to convert, containing fields with metadata.
    
    Returns
    -------
    PythonSchema
        A new schema mapping keys to Python types.
    
    Examples
    --------
    >>> arrow_schema = pa.schema([pa.field('name', pa.string()), pa.field('age', pa.int64())])
    >>> python_schema = from_arrow_schema_to_python_schema(arrow_schema)
    >>> print(python_schema)
    {'name': <class 'str'>, 'age': <class 'int'>}
    """
    semantic_schema = from_arrow_schema_to_semantic_schema(arrow_schema)
    return from_semantic_schema_to_python_schema(semantic_schema, semantic_type_registry)