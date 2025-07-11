from abc import ABC, abstractmethod
from typing import Any
from orcapod.protocols import data_protocols as dp
import logging
from orcapod.data.streams import KernelStream
from orcapod.data.base import LabeledContentIdentifiableBase
from orcapod.data.trackers import DEFAULT_TRACKER_MANAGER
from orcapod.types import TypeSpec

logger = logging.getLogger(__name__)


def get_tracker_manager() -> dp.TrackerManager: ...


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
        fixed_input_streams: tuple[dp.Stream, ...] | None = None,
        label: str | None = None,
        skip_tracking: bool = False,
        tracker_manager: dp.TrackerManager | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._label = label
        self._skip_tracking = skip_tracking
        self._tracker_manager = tracker_manager or DEFAULT_TRACKER_MANAGER
        self.fixed_input_streams = fixed_input_streams

    def resolve_input_streams(self, *streams: dp.Stream) -> tuple[dp.Stream, ...]:
        """
        Resolve the input streams for the kernel. If the kernel has fixed input streams,
        it returns those. Otherwise, it returns the provided streams.
        """
        if self.fixed_input_streams is not None:
            if len(streams) != 0:
                raise ValueError(
                    f"{self.__class__.__name__} has fixed input streams. Additional streams cannot be accepted."
                )
            return self.fixed_input_streams
        return streams

    def pre_processing_step(self, *streams: dp.Stream) -> tuple[dp.Stream, ...]:
        """
        Pre-processing step that can be overridden by subclasses to perform any necessary pre-processing
        on the input streams before the main computation. This is useful if you need to modify the input streams
        or perform any other operations before the main computation. Critically, any Kernel/Pod invocations in the
        pre-processing step will be tracked separately from the main computation in forward.
        By default, it returns the input streams unchanged.
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

    def track_invocation(self, *streams: dp.Stream) -> None:
        """
        Track the invocation of the kernel with the provided streams.
        This is a convenience method that calls record_kernel_invocation.
        """
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_kernel_invocation(self, streams)

    def __call__(
        self, *streams: dp.Stream, label: str | None = None, **kwargs
    ) -> dp.LiveStream:
        streams = self.resolve_input_streams(*streams)
        processed_streams = self.pre_processing_step(*streams)
        self.validate_inputs(*processed_streams)
        output_stream = self.prepare_output_stream(*processed_streams, label=label)
        self.track_invocation(*processed_streams)
        return output_stream

    @abstractmethod
    def forward(self, *streams: dp.Stream) -> dp.Stream:
        """
        Trigger the main computation of the kernel on a collection of streams.
        This method is called when the kernel is invoked with a collection of streams.
        Subclasses should override this method to provide the kernel with its unique behavior
        """

    @abstractmethod
    def output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]: ...

    def __repr__(self):
        return self.__class__.__name__

    def __str__(self):
        if self._label is not None:
            return f"{self.__class__.__name__}({self._label})"
        return self.__class__.__name__

    def identity_structure(self, *streams: dp.Stream) -> Any:
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
        logger.warning(
            f"Identity structure not implemented for {self.__class__.__name__}"
        )
        return (self.__class__.__name__,) + streams
