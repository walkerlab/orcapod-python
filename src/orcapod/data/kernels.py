from abc import ABC, abstractmethod
from collections.abc import Collection
from typing import Any
from orcapod.protocols import data_protocols as dp
import logging
from orcapod.data.streams import KernelStream
from orcapod.data.base import LabeledContentIdentifiableBase
from orcapod.data.context import DataContext
from orcapod.data.trackers import DEFAULT_TRACKER_MANAGER
from orcapod.types import TypeSpec

logger = logging.getLogger(__name__)


class TrackedKernelBase(ABC, LabeledContentIdentifiableBase):
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
        data_context: str | DataContext | None = None,
        skip_tracking: bool = False,
        tracker_manager: dp.TrackerManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._label = label

        self._data_context = DataContext.resolve_data_context(data_context)

        self._skip_tracking = skip_tracking
        self._tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER

    @property
    def data_context(self) -> DataContext:
        return self._data_context

    @property
    def data_context_key(self) -> str:
        """Return the data context key."""
        return self._data_context.context_key

    @property
    @abstractmethod
    def kernel_id(self) -> tuple[str, ...]: ...

    def pre_kernel_processing(self, *streams: dp.Stream) -> tuple[dp.Stream, ...]:
        """
        Pre-processing step that can be overridden by subclasses to perform any necessary pre-processing
        on the input streams before the main computation. This is useful if you need to modify the input streams
        or perform any other operations before the main computation. Critically, any Kernel/Pod invocations in the
        pre-processing step will be tracked outside of the computation in the kernel.
        Default implementation is a no-op, returning the input streams unchanged.
        """
        return streams

    @abstractmethod
    def validate_inputs(self, *streams: dp.Stream) -> None: ...

    def prepare_output_stream(
        self, *streams: dp.Stream, label: str | None = None
    ) -> dp.LiveStream:
        """
        Prepare the output stream for the kernel invocation.
        This method is called after the main computation is performed.
        It creates a KernelStream with the provided streams and label.
        """
        return KernelStream(source=self, upstreams=streams, label=label)

    def track_invocation(self, *streams: dp.Stream, label: str | None = None) -> None:
        """
        Track the invocation of the kernel with the provided streams.
        This is a convenience method that calls record_kernel_invocation.
        """
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_kernel_invocation(self, streams, label=label)

    def __call__(
        self, *streams: dp.Stream, label: str | None = None, **kwargs
    ) -> dp.LiveStream:
        processed_streams = self.pre_kernel_processing(*streams)
        self.validate_inputs(*processed_streams)
        output_stream = self.prepare_output_stream(*processed_streams, label=label)
        self.track_invocation(*processed_streams, label=label)
        return output_stream

    @abstractmethod
    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Trigger the main computation of the kernel on a collection of streams.
        This method is called when the kernel is invoked with a collection of streams.
        Subclasses should override this method to provide the kernel with its unique behavior
        """

    def output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        processed_streams = self.pre_kernel_processing(*streams)
        self.validate_inputs(*processed_streams)
        return self.kernel_output_types(*processed_streams)

    @abstractmethod
    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]: ...

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        if self._label is not None:
            return f"{self.__class__.__name__}({self._label})"
        return self.__class__.__name__

    @abstractmethod
    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any: ...

    def identity_structure(self, streams: Collection[dp.Stream] | None = None) -> Any:
        # Default implementation of identity_structure for the kernel only
        # concerns the kernel class and the streams if present. Subclasses of
        # Kernels should override this method to provide a more meaningful
        # representation of the kernel. Note that kernel must provide the notion
        # of identity under possibly two distinct contexts:
        # 1) identity of the kernel in itself when invoked without any stream
        # 2) identity of the specific invocation of the kernel with a collection of streams
        # While the latter technically corresponds to the identity of the invocation and not
        # the kernel, only kernel can provide meaningful information as to the uniqueness of
        # the invocation as only kernel would know if / how the input stream(s) alter the identity
        # of the invocation. For example, if the kernel corresponds to an commutative computation
        # and therefore kernel K(x, y) == K(y, x), then the identity structure must reflect the
        # equivalence of the two by returning the same identity structure for both invocations.
        # This can be achieved, for example, by returning a set over the streams instead of a tuple.
        if streams is not None:
            streams = self.pre_kernel_processing(*streams)
        return self.kernel_identity_structure(streams)


class WrappedKernel(TrackedKernelBase):
    """
    A wrapper for a kernel that allows it to be used as a stream source.
    This is useful for cases where you want to use a kernel as a source of data
    in a pipeline or other data processing context.
    """

    def __init__(self, kernel: dp.Kernel, **kwargs) -> None:
        # TODO: handle fixed input stream already set on the kernel
        super().__init__(**kwargs)
        self.kernel = kernel

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return self.kernel.kernel_id

    def computed_label(self) -> str | None:
        """
        Compute a label for this kernel based on its content.
        If label is not explicitly set for this kernel and computed_label returns a valid value,
        it will be used as label of this kernel.
        """
        return self.kernel.label

    def forward(self, *streams: dp.Stream) -> dp.Stream:
        return self.kernel.forward(*streams)

    def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
        return self.kernel.output_types(*streams)

    def validate_inputs(self, *streams: dp.Stream) -> None:
        pass

    def __repr__(self):
        return f"WrappedKernel({self.kernel!r})"

    def __str__(self):
        return f"WrappedKernel:{self.kernel!s}"

    def kernel_identity_structure(
        self, streams: Collection[dp.Stream] | None = None
    ) -> Any:
        return self.kernel.identity_structure(streams)
