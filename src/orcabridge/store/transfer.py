# Implements transfer data store that lets you transfer memoized packets between data stores.

from orcabridge.store.types import DataStore
from orcabridge.types import Packet


class TransferDataStore(DataStore):
    """
    A data store that allows transferring memoized packets between different data stores.
    This is useful for moving data between different storage backends.
    """

    def __init__(self, source_store: DataStore, target_store: DataStore) -> None:
        self.source_store = source_store
        self.target_store = target_store

    def transfer(self, store_name: str, content_hash: str, packet: Packet) -> Packet:
        """
        Transfer a memoized packet from the source store to the target store.
        """
        retrieved_packet = self.source_store.retrieve_memoized(
            store_name, content_hash, packet
        )
        if retrieved_packet is None:
            raise ValueError("Packet not found in source store.")

        return self.target_store.memoize(
            store_name, content_hash, packet, retrieved_packet
        )

    def retrieve_memoized(
        self, store_name: str, content_hash: str, packet: Packet
    ) -> Packet | None:
        """
        Retrieve a memoized packet from the target store.
        """
        # Try retrieving from the target store first
        memoized_packet = self.target_store.retrieve_memoized(
            store_name, content_hash, packet
        )
        if memoized_packet is not None:
            return memoized_packet

        # If not found, try retrieving from the source store
        memoized_packet = self.source_store.retrieve_memoized(
            store_name, content_hash, packet
        )
        if memoized_packet is not None:
            # Memoize the packet in the target store as part of the transfer
            self.target_store.memoize(store_name, content_hash, packet, memoized_packet)

        return memoized_packet

    def memoize(
        self,
        store_name: str,
        content_hash: str,
        packet: Packet,
        output_packet: Packet,
    ) -> Packet:
        """
        Memoize a packet in the target store.
        """
        return self.target_store.memoize(
            store_name, content_hash, packet, output_packet
        )
