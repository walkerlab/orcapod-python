import json
import logging
import shutil
from os import PathLike
from pathlib import Path

from orcapod.hashing.legacy_core import hash_packet
from orcapod.hashing.types import LegacyPacketHasher
from orcapod.hashing.defaults import get_default_composite_file_hasher
from orcapod.databases.legacy.types import DataStore
from orcapod.types import Packet, PacketLike

logger = logging.getLogger(__name__)


class NoOpDataStore(DataStore):
    """
    An empty data store that does not store anything.
    This is useful for testing purposes or when no memoization is needed.
    """

    def __init__(self):
        """
        Initialize the NoOpDataStore.
        This does not require any parameters.
        """
        pass

    def memoize(
        self,
        function_name: str,
        function_hash: str,
        packet: PacketLike,
        output_packet: PacketLike,
        overwrite: bool = False,
    ) -> PacketLike:
        return output_packet

    def retrieve_memoized(
        self, function_name: str, function_hash: str, packet: PacketLike
    ) -> PacketLike | None:
        return None


class DirDataStore(DataStore):
    def __init__(
        self,
        store_dir: str | PathLike = "./pod_data",
        packet_hasher: LegacyPacketHasher | None = None,
        copy_files=True,
        preserve_filename=True,
        overwrite=False,
        supplement_source=False,
        legacy_mode=False,
        legacy_algorithm="sha256",
    ) -> None:
        self.store_dir = Path(store_dir)
        # Create the data directory if it doesn't exist
        self.store_dir.mkdir(parents=True, exist_ok=True)
        self.copy_files = copy_files
        self.preserve_filename = preserve_filename
        self.overwrite = overwrite
        self.supplement_source = supplement_source
        if packet_hasher is None and not legacy_mode:
            packet_hasher = get_default_composite_file_hasher(with_cache=True)
        self.packet_hasher = packet_hasher
        self.legacy_mode = legacy_mode
        self.legacy_algorithm = legacy_algorithm

    def memoize(
        self,
        function_name: str,
        function_hash: str,
        packet: PacketLike,
        output_packet: PacketLike,
    ) -> PacketLike:
        if self.legacy_mode:
            packet_hash = hash_packet(packet, algorithm=self.legacy_algorithm)
        else:
            packet_hash = self.packet_hasher.hash_packet(packet)  # type: ignore[no-untyped-call]
        output_dir = self.store_dir / function_name / function_hash / str(packet_hash)
        info_path = output_dir / "_info.json"
        source_path = output_dir / "_source.json"

        if info_path.exists() and not self.overwrite:
            raise ValueError(
                f"Entry for packet {packet} already exists, and will not be overwritten"
            )
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            if self.copy_files:
                new_output_packet = {}
                # copy the files to the output directory
                for key, value in output_packet.items():
                    if not isinstance(value, (str, PathLike)):
                        raise NotImplementedError(
                            f"Pathset that is not a simple path is not yet supported: {value} was given"
                        )
                    if self.preserve_filename:
                        relative_output_path = Path(value).name
                    else:
                        # preserve the suffix of the original if present
                        relative_output_path = key + Path(value).suffix

                    output_path = output_dir / relative_output_path
                    if output_path.exists() and not self.overwrite:
                        logger.warning(
                            f"File {relative_output_path} already exists in {output_path}"
                        )
                        if not self.overwrite:
                            raise ValueError(
                                f"File {relative_output_path} already exists in {output_path}"
                            )
                        else:
                            logger.warning(
                                f"Removing file {relative_output_path} in {output_path}"
                            )
                            shutil.rmtree(output_path)
                    logger.info(f"Copying file {value} to {output_path}")
                    shutil.copy(value, output_path)
                    # register the key with the new path
                    new_output_packet[key] = str(relative_output_path)
                output_packet = new_output_packet
            # store the output packet in a json file
            with open(info_path, "w") as f:
                json.dump(output_packet, f)
            # store the source packet in a json file
            with open(source_path, "w") as f:
                json.dump(packet, f)
            logger.info(f"Stored output for packet {packet} at {output_dir}")

            # retrieve back the memoized packet and return
            # TODO: consider if we want to return the original packet or the memoized one
            retrieved_output_packet = self.retrieve_memoized(
                function_name, function_hash, packet
            )
            if retrieved_output_packet is None:
                raise ValueError(f"Memoized packet {packet} not found after storing it")
            return retrieved_output_packet

    def retrieve_memoized(
        self, function_name: str, function_hash: str, packet: PacketLike
    ) -> Packet | None:
        if self.legacy_mode:
            packet_hash = hash_packet(packet, algorithm=self.legacy_algorithm)
        else:
            assert self.packet_hasher is not None, (
                "Packer hasher should be configured if not in legacy mode"
            )
            packet_hash = self.packet_hasher.hash_packet(packet)
        output_dir = self.store_dir / function_name / function_hash / str(packet_hash)
        info_path = output_dir / "_info.json"
        source_path = output_dir / "_source.json"

        if info_path.exists():
            # TODO: perform better error handling
            try:
                with open(info_path, "r") as f:
                    output_packet = json.load(f)
                # update the paths to be absolute
                for key, value in output_packet.items():
                    # Note: if value is an absolute path, this will not change it as
                    # Pathlib is smart enough to preserve the last occurring absolute path (if present)
                    output_packet[key] = str(output_dir / value)
                logger.info(f"Retrieved output for packet {packet} from {info_path}")
                # check if source json exists -- if not, supplement it
                if self.supplement_source and not source_path.exists():
                    with open(source_path, "w") as f:
                        json.dump(packet, f)
                    logger.info(
                        f"Supplemented source for packet {packet} at {source_path}"
                    )
            except (IOError, json.JSONDecodeError) as e:
                logger.error(
                    f"Error loading memoized output for packet {packet} from {info_path}: {e}"
                )
                return None
            return output_packet
        else:
            logger.info(f"No memoized output found for packet {packet}")
            return None

    def clear_store(self, function_name: str) -> None:
        # delete the folder self.data_dir and its content
        shutil.rmtree(self.store_dir / function_name)

    def clear_all_stores(self, interactive=True, function_name="", force=False) -> None:
        """
        Clear all stores in the data directory.
        This is a dangerous operation -- please double- and triple-check before proceeding!

        Args:
            interactive (bool): If True, prompt the user for confirmation before deleting.
                If False, it will delete only if `force=True`. The user will be prompted
                to type in the full name of the storage (as shown in the prompt)
                to confirm deletion.
            function_name (str): The name of the function to delete. If not using interactive mode,
                this must be set to the store_dir path in order to proceed with the deletion.
            force (bool): If True, delete the store without prompting the user for confirmation.
                If False and interactive is False, the `function_name` must match the store_dir
                for the deletion to proceed.
        """
        # delete the folder self.data_dir and its content
        # This is a dangerous operation -- double prompt the user for confirmation!
        if not force and interactive:
            confirm = input(
                f"Are you sure you want to delete all stores in {self.store_dir}? (y/n): "
            )
            if confirm.lower() != "y":
                logger.info("Aborting deletion of all stores")
                return
            function_name = input(
                f"Type in the function name {self.store_dir} to confirm the deletion: "
            )
            if function_name != str(self.store_dir):
                logger.info("Aborting deletion of all stores")
                return

        if not force and function_name != str(self.store_dir):
            logger.info(f"Aborting deletion of all stores in {self.store_dir}")
            return

        logger.info(f"Deleting all stores in {self.store_dir}")
        try:
            shutil.rmtree(self.store_dir)
        except:
            logger.error(f"Error during the deletion of all stores in {self.store_dir}")
            raise
        logger.info(f"Deleted all stores in {self.store_dir}")
