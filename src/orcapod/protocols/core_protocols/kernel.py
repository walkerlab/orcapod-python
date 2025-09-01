from collections.abc import Collection
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from orcapod.protocols.hashing_protocols import ContentIdentifiable
from orcapod.types import PythonSchema
from orcapod.protocols.core_protocols.base import Labelable
from orcapod.protocols.core_protocols.streams import Stream, LiveStream


@runtime_checkable
class Kernel(ContentIdentifiable, Labelable, Protocol):
    """
    The fundamental unit of computation in Orcapod.

    Kernels are the building blocks of computational graphs, transforming
    zero, one, or more input streams into a single output stream. They
    encapsulate computation logic while providing consistent interfaces
    for validation, type checking, and execution.

    Key design principles:
    - Immutable: Kernels don't change after creation
    - Deterministic: Same inputs always produce same outputs
    - Composable: Kernels can be chained and combined
    - Trackable: All invocations are recorded for lineage
    - Type-safe: Strong typing and validation throughout

    Execution modes:
    - __call__(): Full-featured execution with tracking, returns LiveStream
    - forward(): Pure computation without side effects, returns Stream

    The distinction between these modes enables both production use (with
    full tracking) and testing/debugging (without side effects).
    """

    @property
    def reference(self) -> tuple[str, ...]:
        """
        Reference to the kernel

        The reference is used for caching/storage and tracking purposes.
        As the name indicates, this is how data originating from the kernel will be referred to.


        Returns:
            tuple[str, ...]: Reference for this kernel
        """
        ...

    @property
    def data_context_key(self) -> str:
        """
        Return the context key for this kernel's data processing.

        The context key is used to interpret how data columns should be
        processed and converted. It provides semantic meaning to the data
        being processed by this kernel.

        Returns:
            str: Context key for this kernel's data processing
        """
        ...

    @property
    def last_modified(self) -> datetime | None:
        """
        When the kernel was last modified. For most kernels, this is the timestamp
        of the kernel creation.
        """
        ...

    def __call__(
        self, *streams: Stream, label: str | None = None, **kwargs
    ) -> LiveStream:
        """
        Main interface for kernel invocation with full tracking and guarantees.

        This is the primary way to invoke kernels in production. It provides
        a complete execution pipeline:
        1. Validates input streams against kernel requirements
        2. Registers the invocation with the computational graph
        3. Calls forward() to perform the actual computation
        4. Ensures the result is a LiveStream that stays current

        The returned LiveStream automatically stays up-to-date with its
        upstream dependencies, making it suitable for real-time processing
        and reactive applications.

        Args:
            *streams: Input streams to process (can be empty for source kernels)
            label: Optional label for this invocation (overrides kernel.label)
            **kwargs: Additional arguments for kernel configuration

        Returns:
            LiveStream: Live stream that stays up-to-date with upstreams

        Raises:
            ValidationError: If input streams are invalid for this kernel
            TypeMismatchError: If stream types are incompatible
            ValueError: If required arguments are missing
        """
        ...

    def forward(self, *streams: Stream) -> Stream:
        """
        Perform the actual computation without side effects.

        This method contains the core computation logic and should be
        overridden by subclasses. It performs pure computation without:
        - Registering with the computational graph
        - Performing validation (caller's responsibility)
        - Guaranteeing result type (may return static or live streams)

        The returned stream must be accurate at the time of invocation but
        need not stay up-to-date with upstream changes. This makes forward()
        suitable for:
        - Testing and debugging
        - Batch processing where currency isn't required
        - Internal implementation details

        Args:
            *streams: Input streams to process

        Returns:
            Stream: Result of the computation (may be static or live)
        """
        ...

    def output_types(
        self, *streams: Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Determine output types without triggering computation.

        This method performs type inference based on input stream types,
        enabling efficient type checking and stream property queries.
        It should be fast and not trigger any expensive computation.

        Used for:
        - Pre-execution type validation
        - Query planning and optimization
        - Schema inference in complex pipelines
        - IDE support and developer tooling

        Args:
            *streams: Input streams to analyze

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types) for output

        Raises:
            ValidationError: If input types are incompatible
            TypeError: If stream types cannot be processed
        """
        ...

    def validate_inputs(self, *streams: Stream) -> None:
        """
        Validate input streams, raising exceptions if incompatible.

        This method is called automatically by __call__ before computation
        to provide fail-fast behavior. It should check:
        - Number of input streams
        - Stream types and schemas
        - Any kernel-specific requirements
        - Business logic constraints

        The goal is to catch errors early, before expensive computation
        begins, and provide clear error messages for debugging.

        Args:
            *streams: Input streams to validate

        Raises:
            ValidationError: If streams are invalid for this kernel
            TypeError: If stream types are incompatible
            ValueError: If stream content violates business rules
        """
        ...

    def identity_structure(self, streams: Collection[Stream] | None = None) -> Any:
        """
        Generate a unique identity structure for this kernel and/or kernel invocation.
        When invoked without streams, it should return a structure
        that uniquely identifies the kernel itself (e.g., class name, parameters).
        When invoked with streams, it should include the identity of the streams
        to distinguish different invocations of the same kernel.

        This structure is used for:
        - Caching and memoization
        - Debugging and error reporting
        - Tracking kernel invocations in computational graphs

        Args:
            streams: Optional input streams for this invocation. If None, identity_structure is
                based solely on the kernel. If streams are provided, they are included in the identity
                to differentiate between different invocations of the same kernel.

        Returns:
            Any: Unique identity structure (e.g., tuple of class name and stream identities)
        """
        ...
