from .types import Tag, Packet
from typing import Optional, Collection
from pathlib import Path
from .utils.hash import hash_dict
import shutil
import logging
import json

logger = logging.getLogger(__name__)


class DataStore:
    def memoize(
        self,
        store_name: str,
        fp_hash: int,
        packet: Packet,
        output_packet: Packet,
        overwrite: bool = False,
    ) -> Packet: ...

    def retrieve_memoized(
        self, store_name: str, fp_hash: int, packet: Packet
    ) -> Optional[Packet]: ...


class NoOpDataStore(DataStore):
    """
    An empty data store that does not store anything.
    This is useful for testing purposes or when no memoization is needed.
    """

    def memoize(
        self,
        store_name: str,
        fp_hash: int,
        packet: Packet,
        output_packet: Packet,
        overwrite: bool = False,
    ) -> Packet:
        return output_packet

    def retrieve_memoized(
        self, store_name: str, fp_hash: int, packet: Packet
    ) -> Optional[Packet]:
        return None


class DirDataStore(DataStore):
    def __init__(
        self,
        store_dir="./pod_data",
        copy_files=True,
        preserve_filename=True,
    ) -> None:
        self.store_dir = Path(store_dir)
        # Create the data directory if it doesn't exist
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.copy_files = copy_files
        self.preserve_filename = preserve_filename

    def memoize(
        self,
        store_name: str,
        fp_hash: int,
        packet: Packet,
        output_packet: Packet,
        overwrite: bool = False,
    ) -> Packet:

        packet_hash = hash_dict(packet)
        output_dir = self.store_dir / store_name / str(fp_hash) / str(packet_hash)
        info_path = output_dir / "_info.json"

        if info_path.exists() and not overwrite:
            raise ValueError(
                f"Entry for packet {packet} already exists, and will not be overwritten"
            )
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            if self.copy_files:
                new_output_packet = {}
                # copy the files to the output directory
                for key, value in output_packet.items():
                    if self.preserve_filename:
                        relative_output_path = Path(value).name
                        if (output_dir / relative_output_path).exists():
                            raise ValueError(
                                f"File {relative_output_path} already exists in {output_path}"
                            )
                    else:
                        # preserve the suffix of the original if present
                        relative_output_path = key + Path(value).suffix

                    output_path = output_dir / relative_output_path
                    if output_path.exists() and not overwrite:
                        # TODO: handle case where it's a directory
                        raise ValueError(
                            f"File {relative_output_path} already exists in {output_path}"
                        )
                    shutil.copy(value, output_path)
                    # register the key with the new path
                    new_output_packet[key] = str(relative_output_path)
                output_packet = new_output_packet
            # store the packet in a json file
            with open(info_path, "w") as f:
                json.dump(output_packet, f)
            logger.info(f"Stored output for packet {packet} at {output_path}")

            # retrieve back the memoized packet and return
            # TODO: consider if we want to return the original packet or the memoized one
            output_packet = self.retrieve_memoized(store_name, fp_hash, packet)
            if output_packet is None:
                raise ValueError(f"Memoized packet {packet} not found after storing it")

            return output_packet

    def retrieve_memoized(
        self, store_name: str, fp_hash: int, packet: Packet
    ) -> Optional[Packet]:
        packet_hash = hash_dict(packet)
        output_dir = self.store_dir / store_name / str(fp_hash) / str(packet_hash)
        info_path = output_dir / "_info.json"

        if info_path.exists():
            with open(info_path, "r") as f:
                output_packet = json.load(f)
            # update the paths to be absolute
            for key, value in output_packet.items():
                output_packet[key] = str(output_dir / value)
            logger.info(f"Retrieved output for packet {packet} from {info_path}")
            return output_packet
        else:
            logger.info(f"No memoized output found for packet {packet}")
            return None

    def clear_store(self, store_name: str) -> None:
        # delete the folder self.data_dir and its content
        shutil.rmtree(self.store_dir / store_name)

    def clear_all_stores(self) -> None:
        # delete the folder self.data_dir and its content
        shutil.rmtree(self.store_dir)
