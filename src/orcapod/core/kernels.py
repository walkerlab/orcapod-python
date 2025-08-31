from abc import abstractmethod
from collections.abc import Collection
from datetime import datetime, timezone
from typing import Any
from orcapod.protocols import core_protocols as cp
import logging
from orcapod.core.streams import KernelStream
from orcapod.core.base import LabeledContentIdentifiableBase
from orcapod.core.trackers import DEFAULT_TRACKER_MANAGER
from orcapod.types import PythonSchema

logger = logging.getLogger(__name__)


class TrackedKernelBase(LabeledContentIdentifiableBase):
    """
    Kernel defines the fundamental unit of computation that can be performed on zero, one or more streams of data.
    It is the base class for all computations and transformations that can be performed on a collection of streams
    (including an empty collection).
    A kernel is defined as a callable that takes a (possibly empty) collection of streams as the input
    and returns a new stream as output (note that output stream is always singular).
    Each "invocation" of the kernel on a collection of streams is assigned a unique ID.
    The corresponding invocation information is stored as Invocation object and attached to the output stream
    for computational graph tracking.
    """

    def __init__(
        self,
        label: str | None = None,
        skip_tracking: bool = False,
        tracker_manager: cp.TrackerManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._label = label

        self._skip_tracking = skip_tracking
        self._tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self._last_modified = None
        self._kernel_hash = None
        self._set_modified_time()

    @property
    def reference(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (
            f"{self.__class__.__name__}",
            self.content_hash().to_hex(),
        )

    @property
    def last_modified(self) -> datetime | None:
        """
        When the kernel was last modified. For most kernels, this is the timestamp
        of the kernel creation.
        """
        return self._last_modified

    # TODO: reconsider making this a public method
    def _set_modified_time(
        self, timestamp: datetime | None = None, invalidate: bool = False
    ) -> None:
        """
        Sets the last modified time of the kernel.
        If `invalidate` is True, it resets the last modified time to None to indicate unstable state that'd signal downstream
        to recompute when using the kernel. Othewrise, sets the last modified time to the current time or to the provided timestamp.
        """
        if invalidate:
            self._last_modified = None
            return

        if timestamp is not None:
            self._last_modified = timestamp
        else:
            self._last_modified = datetime.now(timezone.utc)

    @abstractmethod
    def kernel_output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Return the output types of the kernel given the input streams.
        """
        ...

    def output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        processed_streams = self.pre_kernel_processing(*streams)
        self.validate_inputs(*processed_streams)
        return self.kernel_output_types(
            *processed_streams, include_system_tags=include_system_tags
        )

    @abstractmethod
    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        """
        Identity structure for this kernel. Input stream(s), if present, have already been preprocessed
        and validated.
        """
        ...

    def identity_structure(self, streams: Collection[cp.Stream] | None = None) -> Any:
        """
        Default implementation of identity_structure for the kernel only
        concerns the kernel class and the streams if present. Subclasses of
        Kernels should override this method to provide a more meaningful
        representation of the kernel. Note that kernel must provide the notion
        of identity under possibly two distinct contexts:
        1) identity of the kernel in itself when invoked without any stream
        2) identity of the specific invocation of the kernel with a collection of streams
        While the latter technically corresponds to the identity of the invocation and not
        the kernel, only kernel can provide meaningful information as to the uniqueness of
        the invocation as only kernel would know if / how the input stream(s) alter the identity
        of the invocation. For example, if the kernel corresponds to an commutative computation
        and therefore kernel K(x, y) == K(y, x), then the identity structure must reflect the
        equivalence of the two by returning the same identity structure for both invocations.
        This can be achieved, for example, by returning a set over the streams instead of a tuple.
        """
        if streams is not None:
            streams = self.pre_kernel_processing(*streams)
            self.validate_inputs(*streams)
        return self.kernel_identity_structure(streams)

    @abstractmethod
    def forward(self, *streams: cp.Stream) -> cp.Stream:
        """
        Trigger the main computation of the kernel on a collection of streams.
        This method is called when the kernel is invoked with a collection of streams.
        Subclasses should override this method to provide the kernel with its unique behavior
        """

    def pre_kernel_processing(self, *streams: cp.Stream) -> tuple[cp.Stream, ...]:
        """
        Pre-processing step that can be overridden by subclasses to perform any necessary pre-processing
        on the input streams before the main computation. This is useful if you need to modify the input streams
        or perform any other operations before the main computation. Critically, any Kernel/Pod invocations in the
        pre-processing step will be tracked outside of the computation in the kernel.
        Default implementation is a no-op, returning the input streams unchanged.
        """
        return streams

    @abstractmethod
    def validate_inputs(self, *streams: cp.Stream) -> None:
        """
        Validate the input streams before the main computation but after the pre-kernel processing
        """
        ...

    def prepare_output_stream(
        self, *streams: cp.Stream, label: str | None = None
    ) -> KernelStream:
        """
        Prepare the output stream for the kernel invocation.
        This method is called after the main computation is performed.
        It creates a KernelStream with the provided streams and label.
        """
        return KernelStream(source=self, upstreams=streams, label=label)

    def track_invocation(self, *streams: cp.Stream, label: str | None = None) -> None:
        """
        Track the invocation of the kernel with the provided streams.
        This is a convenience method that calls record_kernel_invocation.
        """
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_kernel_invocation(self, streams, label=label)

    def __call__(
        self, *streams: cp.Stream, label: str | None = None, **kwargs
    ) -> KernelStream:
        processed_streams = self.pre_kernel_processing(*streams)
        self.validate_inputs(*processed_streams)
        output_stream = self.prepare_output_stream(*processed_streams, label=label)
        self.track_invocation(*processed_streams, label=label)
        return output_stream

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        if self._label is not None:
            return f"{self.__class__.__name__}({self._label})"
        return self.__class__.__name__


class WrappedKernel(TrackedKernelBase):
    """
    A wrapper for a kernels useful when you want to use an existing kernel
    but need to provide some extra functionality.

    Default implementation provides a simple passthrough to the wrapped kernel.
    If you want to provide a custom behavior, be sure to override the methods
    that you want to change. Note that the wrapped kernel must implement the
    `Kernel` protocol. Refer to `orcapod.protocols.data_protocols.Kernel` for more details.
    """

    def __init__(self, kernel: cp.Kernel, **kwargs) -> None:
        # TODO: handle fixed input stream already set on the kernel
        super().__init__(**kwargs)
        self.kernel = kernel

    def computed_label(self) -> str | None:
        """
        Compute a label for this kernel based on its content.
        If label is not explicitly set for this kernel and computed_label returns a valid value,
        it will be used as label of this kernel.
        """
        return self.kernel.label

    @property
    def reference(self) -> tuple[str, ...]:
        return self.kernel.reference

    def kernel_output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        return self.kernel.output_types(
            *streams, include_system_tags=include_system_tags
        )

    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        return self.kernel.identity_structure(streams)

    def validate_inputs(self, *streams: cp.Stream) -> None:
        return self.kernel.validate_inputs(*streams)

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        return self.kernel.forward(*streams)

    def __repr__(self):
        return f"WrappedKernel({self.kernel!r})"

    def __str__(self):
        return f"WrappedKernel:{self.kernel!s}"
