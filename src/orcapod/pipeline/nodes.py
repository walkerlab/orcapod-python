from orcapod.data.kernels import WrappedKernel
from orcapod.data.pods import WrappedPod
from orcapod.protocols import data_protocols as dp


class KernelNode(WrappedKernel):
    """
    A node in the pipeline that represents a kernel.
    This node can be used to execute the kernel and process data streams.
    """

    def __init__(self, kernel: dp.Kernel, **kwargs) -> None:
        super().__init__(kernel=kernel, **kwargs)
        self.kernel = kernel

    def __repr__(self):
        return f"KernelNode(kernel={self.kernel!r})"

    def __str__(self):
        return f"KernelNode:{self.kernel!s}"


class PodNode(WrappedPod):
    def __init__(self, pod: dp.Pod, **kwargs) -> None:
        super().__init__(pod=pod, **kwargs)
        self.pod = pod

    def __repr__(self):
        return f"PodNode(pod={self.pod!r})"

    def __str__(self):
        return f"PodNode:{self.pod!s}"
