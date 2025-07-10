# from typing import TypeAlias
# from collections.abc import Collection, Mapping
# from pathlib import Path
# import logging
# import os

# logger = logging.getLogger(__name__)


# # class TypeSpec(dict[str, DataType]):
# #     def __init__(self, *args, **kwargs):
# #         """
# #         TypeSpec is a mapping of parameter names to their types.
# #         It can be used to define the expected types of parameters in a function or a pod.
# #         """
# #         super().__init__(*args, **kwargs)


# # Convenience alias for anything pathlike
# PathLike: TypeAlias = str | os.PathLike

# # an (optional) string or a collection of (optional) string values
# # Note that TagValue can be nested, allowing for an arbitrary depth of nested lists
# TagValue: TypeAlias = int | str | None | Collection["TagValue"]

# # the top level tag is a mapping from string keys to values that can be a string or
# # an arbitrary depth of nested list of strings or None
# Tag: TypeAlias = Mapping[str, TagValue]

# # a pathset is a path or an arbitrary depth of nested list of paths
# PathSet: TypeAlias = PathLike | Collection[PathLike | None]

# # Simple data types that we support (with clear Polars correspondence)
# SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes

# ExtendedSupportedPythonData: TypeAlias = SupportedNativePythonData | PathLike

# TypeSpec = dict[str, type]  # Mapping of parameter names to their types

# # Extended data values that can be stored in packets
# # Either the original PathSet or one of our supported simple data types
# DataValue: TypeAlias = (
#     PathSet
#     | SupportedNativePythonData
#     | None
#     | Collection["DataValue"]
#     | Mapping[str, "DataValue"]
# )


# PacketLike = Mapping[str, DataValue]
