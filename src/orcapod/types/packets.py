from orcapod.types.core import DataValue
from typing import TypeAlias, Any
from collections.abc import Mapping, Collection
from orcapod.types.core import TypeSpec, Tag, TypeHandler
from orcapod.types.semantic_type_registry import SemanticTypeRegistry
from orcapod.types import schemas
import pyarrow as pa

# # a packet is a mapping from string keys to data values
PacketLike: TypeAlias = Mapping[str, DataValue]


class Packet(dict[str, DataValue]):
    def __init__(
        self, 
        obj: PacketLike | None = None,
        typespec: TypeSpec | None = None, 
        source_info: dict[str, str|None] | None = None
    ):
        if obj is None:
            obj = {}
        super().__init__(obj)
        if typespec is None:
            from orcapod.types.typespec_utils import get_typespec_from_dict
            typespec = get_typespec_from_dict(self)
        self._typespec = typespec
        if source_info is None:
            source_info = {}
        self._source_info = source_info

    @property
    def typespec(self) -> TypeSpec:
        # consider returning a copy for immutability
        return self._typespec

    @property
    def source_info(self) -> dict[str, str | None]:
        return {key: self._source_info.get(key, None) for key in self.keys()}
    
    @source_info.setter
    def source_info(self, source_info: Mapping[str, str | None]):
        self._source_info = {key: value for key, value in source_info.items() if value is not None}

    def get_composite(self) -> PacketLike:
        composite = self.copy()
        for k, v in self.source_info.items():
            composite[f"_source_info_{k}"] = v
        return composite



# a batch is a tuple of a tag and a list of packets
Batch: TypeAlias = tuple[Tag, Collection[Packet]]


class SemanticPacket(dict[str, Any]):
    """
    A packet that conforms to a semantic schema, mapping string keys to values.
    
    This is used to represent data packets in OrcaPod with semantic types.
    
    Attributes
    ----------
    keys : str
        The keys of the packet.
    values : Any
        The values corresponding to each key.
    
    Examples
    --------
    >>> packet = SemanticPacket(name='Alice', age=30)
    >>> print(packet)
    {'name': 'Alice', 'age': 30}
    """
    def __init__(self, *args, semantic_schema: schemas.SemanticSchema | None = None, source_info: dict[str, str|None] | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = semantic_schema
        if source_info is None:
            source_info = {}
        self.source_info = source_info

    def get_composite(self) -> dict[str, Any]:
        composite = self.copy()
        for k, v in self.source_info.items():
            composite[f"_source_info_{k}"] = v
        return composite


class PacketConverter:
    def __init__(self, typespec: TypeSpec, registry: SemanticTypeRegistry, include_source_info: bool = True):
        self.typespec = typespec
        self.registry = registry

        self.semantic_schema = schemas.from_typespec_to_semantic_schema(
            typespec, registry
        )

        self.include_source_info = include_source_info

        self.arrow_schema = schemas.from_semantic_schema_to_arrow_schema(
            self.semantic_schema, include_source_info=self.include_source_info
        )



        self.key_handlers: dict[str, TypeHandler] = {}

        self.expected_key_set = set(self.typespec.keys())

        for key, (_, semantic_type) in self.semantic_schema.items():
            if semantic_type is None:
                continue
            handler = registry.get_handler_by_semantic_type(semantic_type)
            if handler is None:
                raise ValueError(
                    f"No handler found for semantic type '{semantic_type}' in key '{key}'"
                )
            self.key_handlers[key] = handler

    def _check_key_consistency(self, keys):
        """Check if the provided keys match the expected keys."""
        keys_set = set(keys)
        if keys_set != self.expected_key_set:
            missing_keys = self.expected_key_set - keys_set
            extra_keys = keys_set - self.expected_key_set
            error_parts = []
            if missing_keys:
                error_parts.append(f"Missing keys: {missing_keys}")
            if extra_keys:
                error_parts.append(f"Extra keys: {extra_keys}")

            raise KeyError(f"Keys don't match expected keys. {'; '.join(error_parts)}")

    def from_python_packet_to_semantic_packet(self, python_packet: PacketLike) -> SemanticPacket:
        """Convert a Python packet to a semantic packet.

        Args:
            python_packet: Dictionary mapping parameter names to Python values

        Returns:
            Packet with values converted to semantic types

        Raises:
            KeyError: If packet keys don't match the expected type_info keys
            TypeError: If value type doesn't match expected type
            ValueError: If conversion fails
        """
        # Validate packet keys
        semantic_packet = SemanticPacket(python_packet, semantic_schema=self.semantic_schema, source_info=getattr(python_packet, "source_info", None))
        self._check_key_consistency(set(semantic_packet.keys()))

        # convert from storage to Python types for semantic types
        for key, handler in self.key_handlers.items():
            try:
                semantic_packet[key] = handler.python_to_storage(
                    semantic_packet[key]
                )
            except Exception as e:
                raise ValueError(f"Failed to convert value for '{key}': {e}") from e

        return semantic_packet

        
    
    def from_python_packet_to_arrow_table(self, python_packet: PacketLike) -> pa.Table:
        """Convert a Python packet to an Arrow table.

        Args:
            python_packet: Dictionary mapping parameter names to Python values

        Returns:
            Arrow table representation of the packet
        """
        semantic_packet = self.from_python_packet_to_semantic_packet(python_packet)
        return self.from_semantic_packet_to_arrow_table(semantic_packet)

    def from_semantic_packet_to_arrow_table(self, semantic_packet: SemanticPacket) -> pa.Table:
        """Convert a semantic packet to an Arrow table.

        Args:
            semantic_packet: SemanticPacket with values to convert

        Returns:
            Arrow table representation of the packet
        """
        if self.include_source_info:
            return pa.Table.from_pylist([semantic_packet.get_composite()], schema=self.arrow_schema)
        else:
            return pa.Table.from_pylist([semantic_packet], schema=self.arrow_schema)


    def from_arrow_table_to_semantic_packets(self, arrow_table: pa.Table) -> Collection[SemanticPacket]:
        """Convert an Arrow table to a semantic packet.

        Args:
            arrow_table: Arrow table representation of the packet

        Returns:
            SemanticPacket with values converted from Arrow types
        """
        # TODO: this is a crude check, implement more robust one to check that
        # schema matches what's expected
        if not arrow_table.schema.equals(self.arrow_schema):
            raise ValueError("Arrow table schema does not match expected schema")
        
        semantic_packets_contents = arrow_table.to_pylist()
        
        semantic_packets = []
        for all_packet_content in semantic_packets_contents:
            packet_content = {k: v for k, v in all_packet_content.items() if k in self.expected_key_set}
            source_info = {k.removeprefix('_source_info_'): v for k, v in all_packet_content.items() if k.startswith('_source_info_')}
            semantic_packets.append(SemanticPacket(packet_content, semantic_schema=self.semantic_schema, source_info=source_info))

        return semantic_packets

    def from_semantic_packet_to_python_packet(self, semantic_packet: SemanticPacket) -> Packet:
        """Convert a semantic packet to a Python packet.

        Args:
            semantic_packet: SemanticPacket with values to convert

        Returns:
            Python packet representation of the semantic packet
        """
        # Validate packet keys
        python_packet = Packet(semantic_packet, typespec=self.typespec, source_info=semantic_packet.source_info)
        packet_keys = set(python_packet.keys())
        self._check_key_consistency(packet_keys)

        for key, handler in self.key_handlers.items():
            try:
                python_packet[key] = handler.storage_to_python(
                    python_packet[key]
                )
            except Exception as e:
                raise ValueError(f"Failed to convert value for '{key}': {e}") from e
            
        return python_packet

    def from_arrow_table_to_python_packets(self, arrow_table: pa.Table) -> list[Packet]:
        """Convert an Arrow table to a list of Python packets.

        Args:
            arrow_table: Arrow table representation of the packets

        Returns:
            List of Python packets converted from the Arrow table
        """
        semantic_packets = self.from_arrow_table_to_semantic_packets(arrow_table)
        return [self.from_semantic_packet_to_python_packet(sp) for sp in semantic_packets]

