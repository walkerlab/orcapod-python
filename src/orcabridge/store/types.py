from orcabridge.types import Packet
from typing import Protocol, runtime_checkable


@runtime_checkable
class DataStore(Protocol):
    """
    Protocol for data stores that can memoize and retrieve packets.
    This is used to define the interface for data stores like DirDataStore.
    """

    def __init__(self, *args, **kwargs) -> None: ...
    def memoize(
        self,
        store_name: str,
        content_hash: str,
        packet: Packet,
        output_packet: Packet,
    ) -> Packet: ...

    def retrieve_memoized(
        self, store_name: str, content_hash: str, packet: Packet
    ) -> Packet | None: ...
