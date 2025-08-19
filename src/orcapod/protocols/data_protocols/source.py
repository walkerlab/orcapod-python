from typing import Protocol, runtime_checkable

from orcapod.protocols.data_protocols.kernel import Kernel
from orcapod.protocols.data_protocols.streams import Stream


@runtime_checkable
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

    @property
    def tag_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the tag in the pipeline run records.
        This is used to store the run-associated tag info.
        """
        ...

    @property
    def packet_keys(self) -> tuple[str, ...]:
        """
        Return the keys used for the packet in the pipeline run records.
        This is used to store the run-associated packet info.
        """
        ...

    # def as_lazy_frame(self, sort_by_tags: bool = False) -> "pl.LazyFrame | None": ...

    # def as_polars_df(self, sort_by_tags: bool = False) -> "pl.DataFrame | None": ...

    # def as_pandas_df(self, sort_by_tags: bool = False) -> "pd.DataFrame | None": ...
