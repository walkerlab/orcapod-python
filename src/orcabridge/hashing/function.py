# Provides functions for hashing of a Python function
import inspect
from typing import Callable, Literal
from uuid import UUID

from .core import hash_to_hex, hash_to_int, hash_to_uuid, logger

