import logging
logger = logging.getLogger(__name__)

from pathlib import Path
from typing import List, Optional, Tuple, Iterator, Iterable
from .hash import hash_dict
from .operation import Operation
from .mapper import Join
from .stream import SyncStream, SyncStreamFromGenerator
from .types import Tag, Packet, PodFunction
import json
import shutil


class Pod(Operation):
    pass


class FunctionPod(Pod):
    def __init__(self, function: PodFunction, output_keys: Optional[List[str]] = None) -> None:
        self.function = function
        if output_keys is None:
            output_keys = []
        self.output_keys = output_keys

    def __call__(self, *streams: SyncStream) -> SyncStream:
        # if multiple streams are provided, join them
        if len(streams) > 1:
            stream = streams[0]
            for next_stream in streams[1:]:
                stream = Join()(stream, next_stream)
        elif len(streams) == 1:
            stream = streams[0]
        else:
            raise ValueError("No streams provided to FunctionPod")

        def generator() -> Iterator[Tuple[Tag, Packet]]:
            n_computed = 0
            for tag, packet in stream:
                memoized_packet = self.retrieve_memoized(packet)
                if memoized_packet is not None:
                    yield tag, memoized_packet
                    continue
                values = self.function(**packet)
                if len(self.output_keys) == 0:
                     values = []
                elif len(self.output_keys) == 1:
                    values = [values]
                elif isinstance(values, Iterable):
                    values = list(values)
                elif len(self.output_keys) > 1:
                    raise ValueError("Values returned by function must be a pathlike or a sequence of pathlikes")
                
                if len(values) != len(self.output_keys):
                    raise ValueError("Number of output keys does not match number of values returned by function")
                
                output_packet: Packet = {k: v for k, v in zip(self.output_keys, values)}

                status = self.memoize(packet, output_packet)
                logger.info(f"Store status for element {packet}: {status}")

                n_computed += 1
                logger.info(f"Computed item {n_computed}")
                yield tag, output_packet
        return SyncStreamFromGenerator(generator)
        
    def memoize(self, packet: Packet, output_packet: Packet) -> bool:
        return False
    
    def retrieve_memoized(self, packet: Packet) -> Optional[Packet]:
        return None

class FunctionPodWithDirStorage(FunctionPod):
    """
    A FunctionPod that stores the output in the specified directory.
    The output is stored in a subdirectory named store_name, creating it if it doesn't exist.
    If store_name is None, the function name is used as the directory name.
    The output is stored in a file named based on the hash of the input packet.
    """
    def __init__(self, function: PodFunction, output_keys: Optional[List[str]] = None, store_dir='./pod_data', store_name=None, copy_files=True, preserve_filename=True) -> None:
        super().__init__(function, output_keys)
        self.store_dir = Path(store_dir)
        if store_name is None:
            store_name = self.function.__name__
        self.store_name = store_name
        self.data_dir = self.store_dir / self.store_name
        # Create the data directory if it doesn't exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.copy_files = copy_files
        self.preserve_filename = preserve_filename
        
    def memoize(self, packet: Packet, output_packet: Packet, force=False) -> bool:
        packet_hash = hash_dict(packet)
        output_dir = self.data_dir / f"{packet_hash}"
        info_path = output_dir / "_info.json"
        
        if info_path.exists() and not force:
            logger.info(f"Entry for packet {packet}already exists, and will not be overwritten")
            return False
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            if self.copy_files:
                new_output_packet = {}
                # copy the files to the output directory
                for key, value in output_packet.items():
                    if self.preserve_filename:
                        relative_output_path = Path(value).name
                        if (output_dir / relative_output_path).exists():
                            raise ValueError(f"File {relative_output_path} already exists in {output_path}")
                    else:
                        # preserve the suffix of the original if present
                        relative_output_path = key + Path(value).suffix
                        
                    output_path = output_dir / relative_output_path
                    if output_path.exists() and not force:
                        # TODO: handle case where it's a directory
                        raise ValueError(f"File {relative_output_path} already exists in {output_path}")
                    shutil.copy(value, output_path)
                    # register the key with the new path
                    new_output_packet[key] = str(relative_output_path)
                output_packet = new_output_packet
            # store the packet in a json file
            with open(info_path, 'w') as f:
                json.dump(output_packet, f)
            logger.info(f"Stored output for packet {packet} at {output_path}")
            return True
    
    def retrieve_memoized(self, packet: Packet) -> Optional[Packet]:
        packet_hash = hash_dict(packet)
        output_dir = self.data_dir / f"{packet_hash}"
        info_path = output_dir / "_info.json"
        
        if info_path.exists():
            with open(info_path, 'r') as f:
                output_packet = json.load(f)
            # update the paths to be absolute
            for key, value in output_packet.items():
                output_packet[key] = str(output_dir / value)
            logger.info(f"Retrieved output for packet {packet} from {info_path}")
            return output_packet
        else:
            logger.info(f"No memoized output found for packet {packet}")
            return None