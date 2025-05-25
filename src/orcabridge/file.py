import builtins
import contextlib
import inspect
import os
from pathlib import Path
from typing import Callable, Collection, Dict, Optional, Tuple, Union

from orcabridge.types import Packet, PathSet


@contextlib.contextmanager
def redirect_open(
    mapping: Union[Dict[str, str], Callable[[str], Optional[str]]],
):
    """
    Context manager to intercept file opening operations.

    Args:
        mapping: Either a dictionary mapping original paths to their replacements,
                or a function that takes a path string and returns a replacement path
                (or None to indicate the file should not be opened).

    Raises:
        FileNotFoundError: If using a dictionary and the path is not found in it.
    """
    # Track all places that might store an open() function
    places_to_patch = []

    # 1. Standard builtins.open
    original_builtin_open = builtins.open
    places_to_patch.append((builtins, "open", original_builtin_open))

    # 2. __builtins__ (could be different in some contexts, especially IPython)
    if isinstance(__builtins__, dict) and "open" in __builtins__:
        places_to_patch.append((__builtins__, "open", __builtins__["open"]))

    # 3. Current module's globals (for the calling namespace)
    caller_globals = inspect.currentframe().f_back.f_globals
    if "open" in caller_globals:
        places_to_patch.append((caller_globals, "open", caller_globals["open"]))

    # 4. Check for IPython user namespace
    try:
        import IPython

        ip = IPython.get_ipython()
        if ip and "open" in ip.user_ns:
            places_to_patch.append((ip.user_ns, "open", ip.user_ns["open"]))
    except (ImportError, AttributeError):
        pass

    def patched_open(file, *args, **kwargs):
        # Convert PathLike objects to string if needed
        if hasattr(file, "__fspath__"):
            file_path = os.fspath(file)
        else:
            file_path = str(file)

        if isinstance(mapping, dict):
            if file_path in mapping:
                redirected_path = mapping[file_path]
                print(f"Redirecting '{file_path}' to '{redirected_path}'")
                return original_builtin_open(redirected_path, *args, **kwargs)
            else:
                raise FileNotFoundError(
                    f"Path '{file_path}' not found in redirection mapping"
                )
        else:  # mapping is a function
            redirected_path = mapping(file_path)
            if redirected_path is not None:
                print(f"Redirecting '{file_path}' to '{redirected_path}'")
                return original_builtin_open(redirected_path, *args, **kwargs)
            else:
                raise FileNotFoundError(
                    f"Path '{file_path}' could not be redirected"
                )

    # Apply the patch to all places
    for obj, attr, _ in places_to_patch:
        if isinstance(obj, dict):
            obj[attr] = patched_open
        else:
            setattr(obj, attr, patched_open)

    try:
        yield
    finally:
        # Restore all original functions
        for obj, attr, original in places_to_patch:
            if isinstance(obj, dict):
                obj[attr] = original
            else:
                setattr(obj, attr, original)


def virtual_mount(
    packet: Packet,
) -> Tuple[Packet, Dict[str, str], Dict[str, str]]:
    """
    Visit all pathset within the packet, and convert them to alternative path
    representation. By default, full path is mapped to the file name. If two or
    more paths have the same file name, the second one is suffixed with "_1", the
    third one with "_2", etc. This is useful for creating a virtual mount point
    for a set of files, where the original paths are not important, but the file
    names can be used to identify the files.
    """
    forward_lut = {}  # mapping from original path to new path
    reverse_lut = {}  # mapping from new path to original path
    new_packet = {}

    for key, value in packet.items():
        new_packet[key] = convert_pathset(value, forward_lut, reverse_lut)

    return new_packet, forward_lut, reverse_lut


def convert_pathset(pathset: PathSet, forward_lut, reverse_lut) -> PathSet:
    """
    Convert a pathset to a new pathset. forward_lut and reverse_lut are updated
    with the new paths. The new paths are created by replacing the original paths
    with the new paths in the forward_lut. The reverse_lut is updated with the
    original paths. If name already exists, a suffix is added to the new name to avoid
    collisions.
    """
    if isinstance(pathset, (str, bytes)):
        new_name = Path(pathset).name
        if new_name in reverse_lut:
            # if the name already exists, add a suffix
            i = 1
            while f"{new_name}_{i}" in reverse_lut:
                i += 1
            new_name = f"{new_name}_{i}"
        forward_lut[pathset] = new_name
        reverse_lut[new_name] = pathset
        return new_name
    elif isinstance(pathset, Collection):
        return [convert_pathset(p, forward_lut, reverse_lut) for p in pathset]
    else:
        raise ValueError(
            f"Unsupported pathset type: {type(pathset)}. Expected str, bytes, or Collection."
        )


class WrappedPath:

    def __init__(self, path, name=None):
        self.path = Path(path)
        if name is None:
            name = self.path.name
        self.name = name

    def __fspath__(self) -> Union[str, bytes]:
        return self.path.__fspath__()

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"WrappedPath({self.path}): {self.name}"
