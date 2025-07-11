from typing import Protocol, Any, ContextManager
from orcapod.types import DataValue, TypeSpec
from orcapod.protocols.hashing_protocols import ContentIdentifiable
from collections.abc import Iterator, Collection
import pyarrow as pa
from datetime import datetime


class Datagram(Protocol):
    """
    Base protocol for all data containers in Orcapod.

    Datagrams are the fundamental units of data that flow through the system.
    They provide a unified interface for data access and conversion, ensuring
    consistent behavior across different data types and sources.

    TypeSpec is a dict[str, type] mapping field names to their Python types,
    enabling type checking and validation throughout the computational graph.
    """

    def types(self) -> TypeSpec:
        """
        Return the type specification for this datagram.

        The TypeSpec maps field names to their Python types, enabling
        type checking and validation throughout the system.

        Returns:
            TypeSpec: Dictionary mapping field names to Python types
        """
        ...

    def keys(self) -> Collection[str]:
        """
        Return the available keys/fields in this datagram.

        This provides a way to inspect the structure of the datagram
        without accessing the actual data values.

        Returns:
            Collection[str]: Available field names
        """
        ...

    def as_table(self) -> pa.Table:
        """
        Convert to PyArrow Table format.

        Provides a standardized way to convert datagram content to
        a columnar format suitable for analysis and processing.

        Returns:
            pa.Table: PyArrow table representation
        """
        ...

    def as_dict(self) -> dict[str, DataValue]:
        """
        Convert to dictionary format.

        Provides a simple key-value representation of the datagram
        content, useful for debugging and simple data access.

        Returns:
            dict[str, DataValue]: Dictionary representation
        """
        ...


class Tag(Datagram, Protocol):
    """
    Metadata associated with each data item in a stream.

    Tags carry contextual information about data packets as they flow through
    the computational graph. They are immutable and provide metadata that
    helps with:
    - Data lineage tracking
    - Grouping and aggregation operations
    - Temporal information (timestamps)
    - Source identification
    - Processing context

    Common examples include:
    - Timestamps indicating when data was created/processed
    - Source identifiers showing data origin
    - Processing metadata like batch IDs or session information
    - Grouping keys for aggregation operations
    - Quality indicators or confidence scores
    """

    pass


class Packet(Datagram, Protocol):
    """
    The actual data payload in a stream.

    Packets represent the core data being processed through the computational
    graph. Unlike Tags (which are metadata), Packets contain the actual
    information that computations operate on.

    Packets extend Datagram with additional capabilities for:
    - Source tracking and lineage
    - Content-based hashing for caching
    - Metadata inclusion for debugging

    The distinction between Tag and Packet is crucial for understanding
    data flow: Tags provide context, Packets provide content.
    """

    def as_table(self, include_source: bool = False) -> pa.Table:
        """
        Convert the packet to a PyArrow Table.

        Args:
            include_source: If True, source information is included in the table
                          for debugging and lineage tracking

        Returns:
            pa.Table: PyArrow table representation of packet data
        """
        ...

    def as_dict(self, include_source: bool = False) -> dict[str, DataValue]:
        """
        Convert the packet to a dictionary.

        Args:
            include_source: If True, source information is included in the dictionary
                          for debugging and lineage tracking

        Returns:
            dict[str, DataValue]: Dictionary representation of packet data
        """
        ...

    def content_hash(self) -> str:
        """
        Return a hash of the packet content for caching/comparison.

        This hash should be deterministic and based only on the packet content,
        not on source information or metadata. Used for:
        - Caching computation results
        - Detecting data changes
        - Deduplication operations

        Returns:
            str: Deterministic hash of packet content
        """
        ...

    def source_info(self) -> dict[str, str | None]:
        """
        Return metadata about the packet's source/origin.

        Provides debugging and lineage information about where the packet
        originated. May include information like:
        - File paths for file-based sources
        - Database connection strings
        - API endpoints
        - Processing pipeline information

        Returns:
            dict[str, str | None]: Source metadata for debugging/lineage
        """
        ...

    # def join(self, other: "Packet") -> "Packet": ...

    # def get_as(self, packet_type: PacketType) -> PacketType: ...


class PodFunction(Protocol):
    """
    A function suitable for use in a FunctionPod.

    PodFunctions define the computational logic that operates on individual
    packets within a Pod. They represent pure functions that transform
    data values without side effects.

    These functions are designed to be:
    - Stateless: No dependency on external state
    - Deterministic: Same inputs always produce same outputs
    - Serializable: Can be cached and distributed
    - Type-safe: Clear input/output contracts

    PodFunctions accept named arguments corresponding to packet fields
    and return transformed data values.
    """

    def __call__(self, **kwargs: DataValue) -> None | DataValue:
        """
        Execute the pod function with the given arguments.

        The function receives packet data as named arguments and returns
        either transformed data or None (for filtering operations).

        Args:
            **kwargs: Named arguments mapping packet fields to data values

        Returns:
            None: Filter out this packet (don't include in output)
            DataValue: Single transformed value

        Raises:
            TypeError: If required arguments are missing
            ValueError: If argument values are invalid
        """
        ...


class Labelable(Protocol):
    """
    Protocol for objects that can have a human-readable label.

    Labels provide meaningful names for objects in the computational graph,
    making debugging, visualization, and monitoring much easier. They serve
    as human-friendly identifiers that complement the technical identifiers
    used internally.

    Labels are optional but highly recommended for:
    - Debugging complex computational graphs
    - Visualization and monitoring tools
    - Error messages and logging
    - User interfaces and dashboards
    """

    @property
    def label(self) -> str | None:
        """
        Return the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Returns:
            str: Human-readable label for this object
            None: No label is set (will use default naming)
        """
        ...


class Stream(ContentIdentifiable, Labelable, Protocol):
    """
    Base protocol for all streams in Orcapod.

    Streams represent sequences of (Tag, Packet) pairs flowing through the
    computational graph. They are the fundamental data structure connecting
    kernels and carrying both data and metadata.

    Streams can be either:
    - Static: Immutable snapshots created at a specific point in time
    - Live: Dynamic streams that stay current with upstream dependencies

    All streams provide:
    - Iteration over (tag, packet) pairs
    - Type information and schema access
    - Lineage information (source kernel and upstream streams)
    - Basic caching and freshness tracking
    - Conversion to common formats (tables, dictionaries)
    """

    @property
    def source(self) -> "Kernel | None":
        """
        The kernel that produced this stream.

        This provides lineage information for tracking data flow through
        the computational graph. Root streams (like file sources) may
        have no source kernel.

        Returns:
            Kernel: The source kernel that created this stream
            None: This is a root stream with no source kernel
        """
        ...

    @property
    def upstreams(self) -> tuple["Stream", ...]:
        """
        Input streams used to produce this stream.

        These are the streams that were provided as input to the source
        kernel when this stream was created. Used for dependency tracking
        and cache invalidation.

        Returns:
            tuple[Stream, ...]: Upstream dependency streams (empty for sources)
        """
        ...

    def keys(self) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Available keys/fields in the stream content.

        Returns the field names present in both tags and packets.
        This provides schema information without requiring type details,
        useful for:
        - Schema inspection and exploration
        - Query planning and optimization
        - Field validation and mapping

        Returns:
            tuple[tuple[str, ...], tuple[str, ...]]: (tag_keys, packet_keys)
        """
        ...

    def types(self) -> tuple[TypeSpec, TypeSpec]:
        """
        Type specifications for the stream content.

        Returns the type schema for both tags and packets in this stream.
        This information is used for:
        - Type checking and validation
        - Schema inference and planning
        - Compatibility checking between kernels

        Returns:
            tuple[TypeSpec, TypeSpec]: (tag_types, packet_types)
        """
        ...

    @property
    def last_modified(self) -> datetime | None:
        """
        When the stream's content was last modified.

        This property is crucial for caching decisions and dependency tracking:
        - datetime: Content was last modified at this time (cacheable)
        - None: Content is never stable, always recompute (some dynamic streams)

        Both static and live streams typically return datetime values, but
        live streams update this timestamp whenever their content changes.

        Returns:
            datetime: Timestamp of last modification for most streams
            None: Stream content is never stable (some special dynamic streams)
        """
        ...

    @property
    def is_current(self) -> bool:
        """
        Whether the stream is up-to-date with its dependencies.

        A stream is current if its content reflects the latest state of its
        source kernel and upstream streams. This is used for cache validation
        and determining when refresh is needed.

        For live streams, this should always return True since they stay
        current automatically. For static streams, this indicates whether
        the cached content is still valid.

        Returns:
            bool: True if stream is up-to-date, False if refresh needed
        """
        ...

    def __iter__(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Iterate over (tag, packet) pairs in the stream.

        This is the primary way to access stream data. The behavior depends
        on the stream type:
        - Static streams: Return cached/precomputed data
        - Live streams: May trigger computation and always reflect current state

        Yields:
            tuple[Tag, Packet]: Sequential (tag, packet) pairs
        """
        ...

    def iter_packets(self) -> Iterator[tuple[Tag, Packet]]:
        """
        Alias for __iter__ for explicit packet iteration.

        Provides a more explicit method name when the intent is to iterate
        over packets specifically, improving code readability.

        Yields:
            tuple[Tag, Packet]: Sequential (tag, packet) pairs
        """
        ...

    def as_table(self) -> pa.Table:
        """
        Convert the entire stream to a PyArrow Table.

        Materializes all (tag, packet) pairs into a single table for
        analysis and processing. This operation may be expensive for
        large streams or live streams that need computation.

        Tag fields are prefixed with "_tag_" to avoid naming conflicts
        with packet fields.

        Returns:
            pa.Table: Complete stream data as a PyArrow Table
        """
        ...


class LiveStream(Stream, Protocol):
    """
    A stream that automatically stays up-to-date with its upstream dependencies.

    LiveStream extends the base Stream protocol with capabilities for "up-to-date"
    data flow and reactive computation. Unlike static streams which represent
    snapshots, LiveStreams provide the guarantee that their content always
    reflects the current state of their dependencies.

    Key characteristics:
    - Automatically refresh the stream if changes in the upstreams are detected
    - Track last_modified timestamp when content changes
    - Support manual refresh triggering and invalidation
    - By design, LiveStream would return True for is_current except when auto-update fails.

    LiveStreams are always returned by Kernel.__call__() methods, ensuring
    that normal kernel usage produces live, up-to-date results.

    Caching behavior:
    - last_modified updates whenever content changes
    - Can be cached based on dependency timestamps
    - Invalidation happens automatically when upstreams change

    Use cases:
    - Real-time data processing pipelines
    - Reactive user interfaces
    - Monitoring and alerting systems
    - Dynamic dashboard updates
    - Any scenario requiring current data
    """

    def refresh(self, force: bool = False) -> bool:
        """
        Manually trigger a refresh of this stream's content.

        Forces the stream to check its upstream dependencies and update
        its content if necessary. This is useful when:
        - You want to ensure the latest data before a critical operation
        - You need to force computation at a specific time
        - You're debugging data flow issues
        - You want to pre-compute results for performance
        Args:
            force: If True, always refresh even if the stream is current.
                   If False, only refresh if the stream is not current.

        Returns:
            bool: True if the stream was refreshed, False if it was already current.
        Note: LiveStream refreshes automatically on access, so this
        method may be a no-op for some implementations. However, it's
        always safe to call if you need to control when the cache is refreshed.
        """
        ...

    def invalidate(self) -> None:
        """
        Mark this stream as invalid, forcing a refresh on next access.

        This method is typically called when:
        - Upstream dependencies have changed
        - The source kernel has been modified
        - External data sources have been updated
        - Manual cache invalidation is needed

        The stream will automatically refresh its content the next time
        it's accessed (via iteration, as_table(), etc.).

        This is more efficient than immediate refresh when you know the
        data will be accessed later.
        """
        ...


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

    def output_types(self, *streams: Stream) -> tuple[TypeSpec, TypeSpec]:
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

    def identity_structure(self, *streams: Stream) -> Any:
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
            *streams: Optional input streams for this invocation

        Returns:
            Any: Unique identity structure (e.g., tuple of class name and stream identities)
        """
        ...


class Pod(Kernel, Protocol):
    """
    Specialized kernel for packet-level processing with advanced caching.

    Pods represent a different computational model from regular kernels:
    - Process data one packet at a time (enabling fine-grained parallelism)
    - Support just-in-time evaluation (computation deferred until needed)
    - Provide stricter type contracts (clear input/output schemas)
    - Enable advanced caching strategies (packet-level caching)

    The Pod abstraction is ideal for:
    - Expensive computations that benefit from caching
    - Operations that can be parallelized at the packet level
    - Transformations with strict type contracts
    - Processing that needs to be deferred until access time
    - Functions that operate on individual data items

    Pods use a different execution model where computation is deferred
    until results are actually needed, enabling efficient resource usage
    and fine-grained caching.
    """

    def input_packet_types(self) -> TypeSpec:
        """
        TypeSpec for input packets that this Pod can process.

        Defines the exact schema that input packets must conform to.
        Pods are typically much stricter about input types than regular
        kernels, requiring precise type matching for their packet-level
        processing functions.

        This specification is used for:
        - Runtime type validation
        - Compile-time type checking
        - Schema inference and documentation
        - Input validation and error reporting

        Returns:
            TypeSpec: Dictionary mapping field names to required packet types
        """
        ...

    def output_packet_types(self) -> TypeSpec:
        """
        TypeSpec for output packets that this Pod produces.

        Defines the schema of packets that will be produced by this Pod.
        This is typically determined by the Pod's computational function
        and is used for:
        - Type checking downstream kernels
        - Schema inference in complex pipelines
        - Query planning and optimization
        - Documentation and developer tooling

        Returns:
            TypeSpec: Dictionary mapping field names to output packet types
        """
        ...

    def call(self, tag: Tag, packet: Packet) -> tuple[Tag, Packet | None]:
        """
        Process a single packet with its associated tag.

        This is the core method that defines the Pod's computational behavior.
        It processes one (tag, packet) pair at a time, enabling:
        - Fine-grained caching at the packet level
        - Parallelization opportunities
        - Just-in-time evaluation
        - Filtering operations (by returning None)

        The method signature supports:
        - Tag transformation (modify metadata)
        - Packet transformation (modify content)
        - Filtering (return None to exclude packet)
        - Pass-through (return inputs unchanged)

        Args:
            tag: Metadata associated with the packet
            packet: The data payload to process

        Returns:
            tuple[Tag, Packet | None]:
                - Tag: Output tag (may be modified from input)
                - Packet: Processed packet, or None to filter it out

        Raises:
            TypeError: If packet doesn't match input_packet_types
            ValueError: If packet data is invalid for processing
        """
        ...


class Source(Kernel, Stream, Protocol):
    """
    Entry point for data into the computational graph.

    Sources are special objects that serve dual roles:
    - As Kernels: Can be invoked to produce streams
    - As Streams: Directly provide data without upstream dependencies

    Sources represent the roots of computational graphs and typically
    interface with external data sources. They bridge the gap between
    the outside world and the Orcapod computational model.

    Common source types:
    - File readers (CSV, JSON, Parquet, etc.)
    - Database connections and queries
    - API endpoints and web services
    - Generated data sources (synthetic data)
    - Manual data input and user interfaces
    - Message queues and event streams

    Sources have unique properties:
    - No upstream dependencies (upstreams is empty)
    - Can be both invoked and iterated
    - Serve as the starting point for data lineage
    - May have their own refresh/update mechanisms
    """

    pass


class Tracker(Protocol):
    """
    Records kernel invocations and stream creation for computational graph tracking.

    Trackers are responsible for maintaining the computational graph by recording
    relationships between kernels, streams, and invocations. They enable:
    - Lineage tracking and data provenance
    - Caching and memoization strategies
    - Debugging and error analysis
    - Performance monitoring and optimization
    - Reproducibility and auditing

    Multiple trackers can be active simultaneously, each serving different
    purposes (e.g., one for caching, another for debugging, another for
    monitoring). This allows for flexible and composable tracking strategies.

    Trackers can be selectively activated/deactivated to control overhead
    and focus on specific aspects of the computational graph.
    """

    def set_active(self, active: bool = True) -> None:
        """
        Set the active state of the tracker.

        When active, the tracker will record all kernel invocations and
        stream creations. When inactive, no recording occurs, reducing
        overhead for performance-critical sections.

        Args:
            active: True to activate recording, False to deactivate
        """
        ...

    def is_active(self) -> bool:
        """
        Check if the tracker is currently recording invocations.

        Returns:
            bool: True if tracker is active and recording, False otherwise
        """
        ...

    def record_kernel_invocation(
        self, kernel: Kernel, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a kernel invocation in the computational graph.

        This method is called whenever a kernel is invoked. The tracker
        should record:
        - The kernel and its properties
        - The input streams that were used as input
        - Timing and performance information
        - Any relevant metadata

        Args:
            kernel: The kernel that was invoked
            upstreams: The input streams used for this invocation
        """
        ...

    def record_pod_invocation(
        self, pod: Pod, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a pod invocation in the computational graph.

        This method is called whenever a pod is invoked. The tracker
        should record:
        - The pod and its properties
        - The upstream streams that were used as input
        - Timing and performance information
        - Any relevant metadata

        Args:
            pod: The pod that was invoked
            upstreams: The input streams used for this invocation
        """
        ...


class TrackerManager(Protocol):
    """
    Manages multiple trackers and coordinates their activity.

    The TrackerManager provides a centralized way to:
    - Register and manage multiple trackers
    - Coordinate recording across all active trackers
    - Provide a single interface for graph recording
    - Enable dynamic tracker registration/deregistration

    This design allows for:
    - Multiple concurrent tracking strategies
    - Pluggable tracking implementations
    - Easy testing and debugging (mock trackers)
    - Performance optimization (selective tracking)
    """

    def get_active_trackers(self) -> list[Tracker]:
        """
        Get all currently active trackers.

        Returns only trackers that are both registered and active,
        providing the list of trackers that will receive recording events.

        Returns:
            list[Tracker]: List of trackers that are currently recording
        """
        ...

    def register_tracker(self, tracker: Tracker) -> None:
        """
        Register a new tracker in the system.

        The tracker will be included in future recording operations
        if it is active. Registration is separate from activation
        to allow for dynamic control of tracking overhead.

        Args:
            tracker: The tracker to register
        """
        ...

    def deregister_tracker(self, tracker: Tracker) -> None:
        """
        Remove a tracker from the system.

        The tracker will no longer receive recording notifications
        even if it is still active. This is useful for:
        - Cleaning up temporary trackers
        - Removing failed or problematic trackers
        - Dynamic tracker management

        Args:
            tracker: The tracker to remove
        """
        ...

    def record_kernel_invocation(
        self, kernel: Kernel, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a stream in all active trackers.

        This method broadcasts the stream recording to all currently
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            stream: The stream to record in all active trackers
        """
        ...

    def record_pod_invocation(
        self, pod: Pod, upstreams: tuple[Stream, ...], label: str | None = None
    ) -> None:
        """
        Record a stream in all active trackers.

        This method broadcasts the stream recording to all currently`
        active and registered trackers. It provides a single point
        of entry for recording events, simplifying kernel implementations.

        Args:
            stream: The stream to record in all active trackers
        """
        ...

    def no_tracking(self) -> ContextManager[None]: ...
