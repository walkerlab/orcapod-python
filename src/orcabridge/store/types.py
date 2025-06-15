from typing import Protocol, runtime_checkable

from orcabridge.types import Packet
import pyarrow as pa


@runtime_checkable
class DataStore(Protocol):
    """
    Protocol for data stores that can memoize and retrieve packets.
    This is used to define the interface for data stores like DirDataStore.
    """

    def __init__(self, *args, **kwargs) -> None: ...
    def memoize(
        self,
        function_name: str,
        function_hash: str,
        packet: Packet,
        output_packet: Packet,
    ) -> Packet: ...

    def retrieve_memoized(
        self, function_name: str, function_hash: str, packet: Packet
    ) -> Packet | None: ...


@runtime_checkable
class ArrowBasedDataStore(Protocol):
    """
    Protocol for data stores that can memoize and retrieve packets.
    This is used to define the interface for data stores like DirDataStore.
    """

    def __init__(self, *args, **kwargs) -> None: ...
    def memoize(
        self,
        function_name: str,
        function_hash: str,
        packet: pa.Table,
        output_packet: pa.Table,
    ) -> pa.Table: ...

    def retrieve_memoized(
        self, function_name: str, function_hash: str, packet: Packet
    ) -> Packet | None: ...
