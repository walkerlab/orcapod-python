# Protocols for pipeline and nodes
from typing import Protocol, runtime_checkable, TYPE_CHECKING
from orcapod.protocols.core_protocols.source import Source
from orcapod.protocols.core_protocols.pods import CachedPod


if TYPE_CHECKING:
    import pyarrow as pa


class Node(Source, Protocol):
    # def record_pipeline_outputs(self):
    #     pass
    ...


@runtime_checkable
class PodNode(CachedPod, Protocol):
    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all tag and packet processed by this Pod.

        This method returns a table containing all packets processed by the Pod,
        including metadata and system columns if requested. It is useful for:
        - Debugging and analysis
        - Auditing and data lineage tracking
        - Performance monitoring

        Args:
            include_system_columns: Whether to include system columns in the output

        Returns:
            pa.Table | None: A table containing all processed records, or None if no records are available
        """
        ...
