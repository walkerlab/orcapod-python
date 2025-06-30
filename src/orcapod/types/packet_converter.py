from orcapod.types.core import TypeSpec, TypeHandler
from orcapod.types.packets import Packet, PacketLike
from orcapod.types.semantic_type_registry import SemanticTypeRegistry, TypeInfo, get_metadata_from_schema, arrow_to_dicts
from typing import Any
from collections.abc import Mapping, Sequence
import pyarrow as pa
import logging

logger = logging.getLogger(__name__)


def is_packet_supported(
    python_type_info: TypeSpec, registry: SemanticTypeRegistry, type_lut: dict | None = None
) -> bool:
    """Check if all types in the packet are supported by the registry or known to the default lut."""
    if type_lut is None:
        type_lut = {}
    return all(
        python_type in registry or python_type in type_lut
        for python_type in python_type_info.values()
    )



class PacketConverter:
    def __init__(self, python_type_spec: TypeSpec, registry: SemanticTypeRegistry):
        self.python_type_spec = python_type_spec
        self.registry = registry

        # Lookup handlers and type info for fast access
        self.handlers: dict[str, TypeHandler] = {}
        self.storage_type_info: dict[str, TypeInfo] = {}

        self.expected_key_set = set(python_type_spec.keys())

        # prepare the corresponding arrow table schema with metadata
        self.keys_with_handlers, self.schema = create_schema_from_python_type_info(
            python_type_spec, registry
        )

        self.semantic_type_lut = get_metadata_from_schema(self.schema, b"semantic_type")

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

    def _to_storage_packet(self, packet: PacketLike) -> dict[str, Any]:
        """Convert packet to storage representation.

        Args:
            packet: Dictionary mapping parameter names to Python values

        Returns:
            Dictionary with same keys but values converted to storage format

        Raises:
            KeyError: If packet keys don't match the expected type_info keys
            TypeError: If value type doesn't match expected type
            ValueError: If conversion fails
        """
        # Validate packet keys
        packet_keys = set(packet.keys())

        self._check_key_consistency(packet_keys)

        # Convert each value
        storage_packet: dict[str, Any] = dict(packet)  # Start with a copy of the packet

        for key, handler in self.keys_with_handlers:
            try:
                storage_packet[key] = handler.python_to_storage(storage_packet[key])
            except Exception as e:
                raise ValueError(f"Failed to convert value for '{key}': {e}") from e

        return storage_packet

    def _from_storage_packet(self, storage_packet: Mapping[str, Any]) -> PacketLike:
        """Convert storage packet back to Python packet.

        Args:
            storage_packet: Dictionary with values in storage format

        Returns:
            Packet with values converted back to Python types

        Raises:
            KeyError: If storage packet keys don't match the expected type_info keys
            TypeError: If value type doesn't match expected type
            ValueError: If conversion fails
        """
        # Validate storage packet keys
        storage_keys = set(storage_packet.keys())

        self._check_key_consistency(storage_keys)

        # Convert each value back to Python type
        packet: PacketLike = dict(storage_packet)

        for key, handler in self.keys_with_handlers:
            try:
                packet[key] = handler.storage_to_python(storage_packet[key])
            except Exception as e:
                raise ValueError(f"Failed to convert value for '{key}': {e}") from e

        return packet

    def to_arrow_table(self, packet: PacketLike | Sequence[PacketLike]) -> pa.Table:
        """Convert packet to PyArrow Table with field metadata.

        Args:
            packet: Dictionary mapping parameter names to Python values

        Returns:
            PyArrow Table with the packet data as a single row
        """
        # Convert packet to storage format
        if not isinstance(packet, Sequence):
            packets = [packet]
        else:
            packets = packet

        storage_packets = [self._to_storage_packet(p) for p in packets]

        # Create arrays
        arrays = []
        for field in self.schema:
            values = [p[field.name] for p in storage_packets]
            array = pa.array(values, type=field.type)
            arrays.append(array)

        return pa.Table.from_arrays(arrays, schema=self.schema)

    def from_arrow_table(
        self, table: pa.Table, verify_semantic_equivalence: bool = True
    ) -> list[Packet]:
        """Convert Arrow table to packet with field metadata.

        Args:
            table: PyArrow Table with metadata

        Returns:
            List of packets converted from the Arrow table
        """
        # Check for consistency in the semantic type mapping:
        semantic_type_info = get_metadata_from_schema(table.schema, b"semantic_type")

        if semantic_type_info != self.semantic_type_lut:
            if not verify_semantic_equivalence:
                logger.warning(
                    "Arrow table semantic types do not match expected type registry. "
                    f"Expected: {self.semantic_type_lut}, got: {semantic_type_info}"
                )
            else:
                raise ValueError(
                    "Arrow table semantic types do not match expected type registry. "
                    f"Expected: {self.semantic_type_lut}, got: {semantic_type_info}"
                )

        # Create packets from the Arrow table
        # TODO: make this more efficient
        storage_packets: list[Packet] = arrow_to_dicts(table)  # type: ignore
        if not self.keys_with_handlers:
            # no special handling required
            return storage_packets

        return [Packet(self._from_storage_packet(packet)) for packet in storage_packets]

