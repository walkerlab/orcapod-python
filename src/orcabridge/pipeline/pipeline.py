import json
import logging
import pickle
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import networkx as nx
import pandas as pd

from orcabridge.core.base import Invocation, Kernel
from orcabridge.hashing import hash_to_hex
from orcabridge.core.tracker import GraphTracker

logger = logging.getLogger(__name__)


class SerializationError(Exception):
    """Raised when pipeline cannot be serialized"""

    pass


class Pipeline(GraphTracker):
    """
    Enhanced pipeline that tracks operations and provides queryable views.
    Replaces the old Tracker with better persistence and view capabilities.
    """

    def __init__(self, name: str | None = None):
        super().__init__()
        self.name = name or f"pipeline_{id(self)}"
        self._view_registry: dict[str, "PipelineView"] = {}
        self._cache_dir = Path(".pipeline_cache") / self.name
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    # Core Pipeline Operations
    def save(self, path: Path | str) -> None:
        """Save complete pipeline state - named functions only"""
        path = Path(path)

        # Validate serializability first
        self._validate_serializable()

        state = {
            "name": self.name,
            "invocation_lut": self.invocation_lut,
            "metadata": {
                "created_at": time.time(),
                "python_version": sys.version_info[:2],
                "orcabridge_version": "0.1.0",  # You can make this dynamic
            },
        }

        # Atomic write
        temp_path = path.with_suffix(".tmp")
        try:
            with open(temp_path, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)
            temp_path.replace(path)
            logger.info(f"Pipeline '{self.name}' saved to {path}")
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

    @classmethod
    def load(cls, path: Path | str) -> "Pipeline":
        """Load complete pipeline state"""
        path = Path(path)

        with open(path, "rb") as f:
            state = pickle.load(f)

        pipeline = cls(state["name"])
        pipeline.invocation_lut = state["invocation_lut"]

        logger.info(f"Pipeline '{pipeline.name}' loaded from {path}")
        return pipeline

    def _validate_serializable(self) -> None:
        """Ensure pipeline contains only serializable operations"""
        issues = []

        for operation, invocations in self.invocation_lut.items():
            # Check for lambda functions
            if hasattr(operation, "function"):
                func = getattr(operation, "function", None)
                if func and hasattr(func, "__name__") and func.__name__ == "<lambda>":
                    issues.append(f"Lambda function in {operation.__class__.__name__}")

            # Test actual serializability
            try:
                pickle.dumps(operation)
            except Exception as e:
                issues.append(f"Non-serializable operation {operation}: {e}")

        if issues:
            raise SerializationError(
                "Pipeline contains non-serializable elements:\n"
                + "\n".join(f"  - {issue}" for issue in issues)
                + "\n\nOnly named functions are supported for serialization."
            )

    # View Management
    def as_view(
        self, renderer: "ViewRenderer", view_id: str | None = None, **kwargs
    ) -> "PipelineView":
        """Get a view of this pipeline using the specified renderer"""
        view_id = (
            view_id
            or f"{renderer.__class__.__name__.lower()}_{len(self._view_registry)}"
        )

        if view_id not in self._view_registry:
            self._view_registry[view_id] = renderer.create_view(
                self, view_id=view_id, **kwargs
            )
        return self._view_registry[view_id]

    def as_dataframe(self, view_id: str = "default", **kwargs) -> "PandasPipelineView":
        """Convenience method for pandas DataFrame view"""
        return self.as_view(PandasViewRenderer(), view_id=view_id, **kwargs)

    def as_graph(self) -> nx.DiGraph:
        """Get the computation graph"""
        return self.generate_graph()

    # Combined save/load with views
    def save_with_views(self, base_path: Path | str) -> dict[str, Path]:
        """Save pipeline and all its views together"""
        base_path = Path(base_path)
        base_path.mkdir(parents=True, exist_ok=True)

        saved_files = {}

        # Save pipeline itself
        pipeline_path = base_path / "pipeline.pkl"
        self.save(pipeline_path)
        saved_files["pipeline"] = pipeline_path

        # Save all views
        for view_id, view in self._view_registry.items():
            view_path = base_path / f"view_{view_id}.pkl"
            view.save(view_path, include_pipeline=False)
            saved_files[f"view_{view_id}"] = view_path

        # Save manifest
        manifest = {
            "pipeline_file": "pipeline.pkl",
            "views": {
                view_id: f"view_{view_id}.pkl" for view_id in self._view_registry.keys()
            },
            "created_at": time.time(),
            "pipeline_name": self.name,
        }

        manifest_path = base_path / "manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
        saved_files["manifest"] = manifest_path

        return saved_files

    @classmethod
    def load_with_views(
        cls, base_path: Path | str
    ) -> tuple["Pipeline", dict[str, "PipelineView"]]:
        """Load pipeline and all its views"""
        base_path = Path(base_path)

        # Load manifest
        manifest_path = base_path / "manifest.json"
        with open(manifest_path, "r") as f:
            manifest = json.load(f)

        # Load pipeline
        pipeline_path = base_path / manifest["pipeline_file"]
        pipeline = cls.load(pipeline_path)

        # Load views with appropriate renderers
        renderers = {
            "PandasPipelineView": PandasViewRenderer(),
            "DataJointPipelineView": DataJointViewRenderer(None),  # Would need schema
        }

        views = {}
        for view_id, view_file in manifest["views"].items():
            view_path = base_path / view_file

            # Load view data to determine type
            with open(view_path, "rb") as f:
                view_data = pickle.load(f)

            # Find appropriate renderer
            view_type = view_data.get("view_type", "PandasPipelineView")
            if view_type in renderers and renderers[view_type].can_load_view(view_data):
                # Load with appropriate view class
                if view_type == "PandasPipelineView":
                    view = PandasPipelineView.load(view_path, pipeline)
                else:
                    view = DataJointPipelineView.load(view_path, pipeline)
            else:
                # Default to pandas view
                view = PandasPipelineView.load(view_path, pipeline)

            views[view_id] = view
            pipeline._view_registry[view_id] = view

        return pipeline, views

    def get_stats(self) -> dict[str, Any]:
        """Get pipeline statistics"""
        total_operations = len(self.invocation_lut)
        total_invocations = sum(len(invs) for invs in self.invocation_lut.values())

        operation_types = {}
        for operation in self.invocation_lut.keys():
            op_type = operation.__class__.__name__
            operation_types[op_type] = operation_types.get(op_type, 0) + 1

        return {
            "name": self.name,
            "total_operations": total_operations,
            "total_invocations": total_invocations,
            "operation_types": operation_types,
            "views": list(self._view_registry.keys()),
        }


# View Renderer Protocol
@runtime_checkable
class ViewRenderer(Protocol):
    """Protocol for all view renderers - uses structural typing"""

    def create_view(
        self, pipeline: "Pipeline", view_id: str, **kwargs
    ) -> "PipelineView":
        """Create a view for the given pipeline"""
        ...

    def can_load_view(self, view_data: dict[str, Any]) -> bool:
        """Check if this renderer can load the given view data"""
        ...


class PandasViewRenderer:
    """Renderer for pandas DataFrame views"""

    def create_view(
        self, pipeline: "Pipeline", view_id: str, **kwargs
    ) -> "PandasPipelineView":
        return PandasPipelineView(pipeline, view_id=view_id, **kwargs)

    def can_load_view(self, view_data: dict[str, Any]) -> bool:
        return view_data.get("view_type") == "PandasPipelineView"


class DataJointViewRenderer:
    """Renderer for DataJoint views"""

    def __init__(self, schema):
        self.schema = schema

    def create_view(
        self, pipeline: "Pipeline", view_id: str, **kwargs
    ) -> "DataJointPipelineView":
        return DataJointPipelineView(pipeline, self.schema, view_id=view_id, **kwargs)

    def can_load_view(self, view_data: dict[str, Any]) -> bool:
        return view_data.get("view_type") == "DataJointPipelineView"


# Base class for all views
class PipelineView(ABC):
    """Base class for all pipeline views"""

    def __init__(self, pipeline: Pipeline, view_id: str):
        self.pipeline = pipeline
        self.view_id = view_id
        self._cache_dir = pipeline._cache_dir / "views"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def save(self, path: Path | str, include_pipeline: bool = True) -> None:
        """Save the view"""
        pass

    @classmethod
    @abstractmethod
    def load(cls, path: Path | str, pipeline: Pipeline | None = None) -> "PipelineView":
        """Load the view"""
        pass

    def _compute_pipeline_hash(self) -> str:
        """Compute hash of current pipeline state for validation"""
        pipeline_state = []
        for operation, invocations in self.pipeline.invocation_lut.items():
            for invocation in invocations:
                pipeline_state.append(invocation.content_hash())
        return hash_to_hex(sorted(pipeline_state))


# Pandas DataFrame-like view
class PandasPipelineView(PipelineView):
    """
    Provides a pandas DataFrame-like interface to pipeline metadata.
    Focuses on tag information for querying and filtering.
    """

    def __init__(
        self,
        pipeline: Pipeline,
        view_id: str = "pandas_view",
        max_records: int = 10000,
        sample_size: int = 100,
    ):
        super().__init__(pipeline, view_id)
        self.max_records = max_records
        self.sample_size = sample_size
        self._cached_data: pd.DataFrame | None = None
        self._build_options = {"max_records": max_records, "sample_size": sample_size}
        self._hash_to_data_map: dict[str, Any] = {}

    @property
    def df(self) -> pd.DataFrame:
        """Access the underlying DataFrame, building if necessary"""
        if self._cached_data is None:
            # Try to load from cache first
            cache_path = self._cache_dir / f"{self.view_id}.pkl"
            if cache_path.exists():
                try:
                    loaded_view = self.load(cache_path, self.pipeline)
                    if self._is_cache_valid(loaded_view):
                        self._cached_data = loaded_view._cached_data
                        self._hash_to_data_map = loaded_view._hash_to_data_map
                        logger.info(f"Loaded view '{self.view_id}' from cache")
                        return self._cached_data
                except Exception as e:
                    logger.warning(f"Failed to load cached view: {e}")

            # Build from scratch
            logger.info(f"Building view '{self.view_id}' from pipeline")
            self._cached_data = self._build_metadata()

            # Auto-save after building
            try:
                self.save(cache_path, include_pipeline=False)
            except Exception as e:
                logger.warning(f"Failed to cache view: {e}")

        return self._cached_data

    def _build_metadata(self) -> pd.DataFrame:
        """Build the metadata DataFrame from pipeline operations"""
        metadata_records = []
        total_records = 0

        for operation, invocations in self.pipeline.invocation_lut.items():
            if total_records >= self.max_records:
                logger.warning(f"Hit max_records limit ({self.max_records})")
                break

            for invocation in invocations:
                try:
                    # Get sample of outputs, not all
                    records = self._extract_metadata_from_invocation(
                        invocation, operation
                    )
                    for record in records:
                        metadata_records.append(record)
                        total_records += 1
                        if total_records >= self.max_records:
                            break

                    if total_records >= self.max_records:
                        break

                except Exception as e:
                    logger.warning(f"Skipping {operation.__class__.__name__}: {e}")
                    # Create placeholder record
                    placeholder = self._create_placeholder_record(invocation, operation)
                    metadata_records.append(placeholder)
                    total_records += 1

        if not metadata_records:
            # Return empty DataFrame with basic structure
            return pd.DataFrame(
                columns=[
                    "operation_name",
                    "operation_hash",
                    "invocation_id",
                    "created_at",
                    "packet_keys",
                ]
            )

        return pd.DataFrame(metadata_records)

    def _extract_metadata_from_invocation(
        self, invocation: Invocation, operation: Kernel
    ) -> list[dict[str, Any]]:
        """Extract metadata records from a single invocation"""
        records = []

        # Try to get sample outputs from the invocation
        try:
            # This is tricky - we need to reconstruct the output stream
            # For now, we'll create a basic record from what we know
            base_record = {
                "operation_name": operation.label or operation.__class__.__name__,
                "operation_hash": invocation.content_hash(),
                "invocation_id": hash(invocation),
                "created_at": time.time(),
                "operation_type": operation.__class__.__name__,
            }

            # Try to get tag and packet info from the operation
            try:
                tag_keys, packet_keys = invocation.keys()
                base_record.update(
                    {
                        "tag_keys": list(tag_keys) if tag_keys else [],
                        "packet_keys": list(packet_keys) if packet_keys else [],
                    }
                )
            except Exception:
                base_record.update(
                    {
                        "tag_keys": [],
                        "packet_keys": [],
                    }
                )

            records.append(base_record)

        except Exception as e:
            logger.debug(f"Could not extract detailed metadata from {operation}: {e}")
            records.append(self._create_placeholder_record(invocation, operation))

        return records

    def _create_placeholder_record(
        self, invocation: Invocation, operation: Kernel
    ) -> dict[str, Any]:
        """Create a placeholder record when extraction fails"""
        return {
            "operation_name": operation.label or operation.__class__.__name__,
            "operation_hash": invocation.content_hash(),
            "invocation_id": hash(invocation),
            "created_at": time.time(),
            "operation_type": operation.__class__.__name__,
            "tag_keys": [],
            "packet_keys": [],
            "is_placeholder": True,
        }

    # DataFrame-like interface
    def __getitem__(self, condition) -> "FilteredPipelineView":
        """Enable pandas-like filtering: view[condition]"""
        df = self.df
        if isinstance(condition, pd.Series):
            filtered_df = df[condition]
        elif callable(condition):
            filtered_df = df[condition(df)]
        else:
            filtered_df = df[condition]

        return FilteredPipelineView(self.pipeline, filtered_df, self._hash_to_data_map)

    def query(self, expr: str) -> "FilteredPipelineView":
        """SQL-like querying: view.query('operation_name == "MyOperation"')"""
        df = self.df
        filtered_df = df.query(expr)
        return FilteredPipelineView(self.pipeline, filtered_df, self._hash_to_data_map)

    def groupby(self, *args, **kwargs) -> "GroupedPipelineView":
        """Group operations similar to pandas groupby"""
        df = self.df
        grouped = df.groupby(*args, **kwargs)
        return GroupedPipelineView(self.pipeline, grouped, self._hash_to_data_map)

    def head(self, n: int = 5) -> pd.DataFrame:
        """Return first n rows"""
        return self.df.head(n)

    def info(self) -> None:
        """Display DataFrame info"""
        return self.df.info()

    def describe(self) -> pd.DataFrame:
        """Generate descriptive statistics"""
        return self.df.describe()

    # Persistence methods
    def save(self, path: Path | str, include_pipeline: bool = True) -> None:
        """Save view, optionally with complete pipeline state"""
        path = Path(path)

        # Build the view data if not cached
        df = self.df

        view_data = {
            "view_id": self.view_id,
            "view_type": self.__class__.__name__,
            "dataframe": df,
            "build_options": self._build_options,
            "hash_to_data_map": self._hash_to_data_map,
            "created_at": time.time(),
            "pipeline_hash": self._compute_pipeline_hash(),
        }

        if include_pipeline:
            view_data["pipeline_state"] = {
                "name": self.pipeline.name,
                "invocation_lut": self.pipeline.invocation_lut,
            }
            view_data["has_pipeline"] = True
        else:
            view_data["pipeline_name"] = self.pipeline.name
            view_data["has_pipeline"] = False

        with open(path, "wb") as f:
            pickle.dump(view_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def load(
        cls, path: Path | str, pipeline: Pipeline | None = None
    ) -> "PandasPipelineView":
        """Load view, reconstructing pipeline if needed"""
        with open(path, "rb") as f:
            view_data = pickle.load(f)

        # Handle pipeline reconstruction
        if view_data["has_pipeline"]:
            pipeline = Pipeline(view_data["pipeline_state"]["name"])
            pipeline.invocation_lut = view_data["pipeline_state"]["invocation_lut"]
        elif pipeline is None:
            raise ValueError(
                "View was saved without pipeline state. "
                "You must provide a pipeline parameter."
            )

        # Reconstruct view
        build_options = view_data.get("build_options", {})
        view = cls(
            pipeline,
            view_id=view_data["view_id"],
            max_records=build_options.get("max_records", 10000),
            sample_size=build_options.get("sample_size", 100),
        )
        view._cached_data = view_data["dataframe"]
        view._hash_to_data_map = view_data.get("hash_to_data_map", {})

        return view

    def _is_cache_valid(self, cached_view: "PandasPipelineView") -> bool:
        """Check if cached view is still valid"""
        try:
            cached_hash = getattr(cached_view, "_pipeline_hash", None)
            current_hash = self._compute_pipeline_hash()
            return cached_hash == current_hash
        except Exception:
            return False

    def invalidate(self) -> None:
        """Force re-rendering on next access"""
        self._cached_data = None
        cache_path = self._cache_dir / f"{self.view_id}.pkl"
        if cache_path.exists():
            cache_path.unlink()


class FilteredPipelineView:
    """Represents a filtered subset of pipeline metadata"""

    def __init__(
        self, pipeline: Pipeline, filtered_df: pd.DataFrame, data_map: dict[str, Any]
    ):
        self.pipeline = pipeline
        self.df = filtered_df
        self._data_map = data_map

    def __getitem__(self, condition):
        """Further filtering"""
        further_filtered = self.df[condition]
        return FilteredPipelineView(self.pipeline, further_filtered, self._data_map)

    def query(self, expr: str):
        """Apply additional query"""
        further_filtered = self.df.query(expr)
        return FilteredPipelineView(self.pipeline, further_filtered, self._data_map)

    def to_pandas(self) -> pd.DataFrame:
        """Convert to regular pandas DataFrame"""
        return self.df.copy()

    def head(self, n: int = 5) -> pd.DataFrame:
        """Return first n rows"""
        return self.df.head(n)

    def __len__(self) -> int:
        return len(self.df)

    def __repr__(self) -> str:
        return f"FilteredPipelineView({len(self.df)} records)"


class GroupedPipelineView:
    """Represents grouped pipeline metadata"""

    def __init__(self, pipeline: Pipeline, grouped_df, data_map: dict[str, Any]):
        self.pipeline = pipeline
        self.grouped = grouped_df
        self._data_map = data_map

    def apply(self, func):
        """Apply function to each group"""
        return self.grouped.apply(func)

    def agg(self, *args, **kwargs):
        """Aggregate groups"""
        return self.grouped.agg(*args, **kwargs)

    def size(self):
        """Get group sizes"""
        return self.grouped.size()

    def get_group(self, name):
        """Get specific group"""
        group_df = self.grouped.get_group(name)
        return FilteredPipelineView(self.pipeline, group_df, self._data_map)


# Basic DataJoint View (simplified implementation)
class DataJointPipelineView(PipelineView):
    """
    Basic DataJoint view - creates tables for pipeline operations
    This is a simplified version - you can expand based on your existing DJ code
    """

    def __init__(self, pipeline: Pipeline, schema, view_id: str = "dj_view"):
        super().__init__(pipeline, view_id)
        self.schema = schema
        self._tables = {}

    def save(self, path: Path | str, include_pipeline: bool = True) -> None:
        """Save DataJoint view metadata"""
        view_data = {
            "view_id": self.view_id,
            "view_type": self.__class__.__name__,
            "schema_database": self.schema.database,
            "table_names": list(self._tables.keys()),
            "created_at": time.time(),
        }

        if include_pipeline:
            view_data["pipeline_state"] = {
                "name": self.pipeline.name,
                "invocation_lut": self.pipeline.invocation_lut,
            }
            view_data["has_pipeline"] = True

        with open(path, "wb") as f:
            pickle.dump(view_data, f)

    @classmethod
    def load(
        cls, path: Path | str, pipeline: Pipeline | None = None
    ) -> "DataJointPipelineView":
        """Load DataJoint view"""
        with open(path, "rb") as f:
            view_data = pickle.load(f)

        # This would need actual DataJoint schema reconstruction
        # For now, return a basic instance
        if pipeline is None:
            raise ValueError("Pipeline required for DataJoint view loading")

        # You'd need to reconstruct the schema here
        view = cls(pipeline, None, view_id=view_data["view_id"])  # schema=None for now
        return view

    def generate_tables(self):
        """Generate DataJoint tables from pipeline - placeholder implementation"""
        # This would use your existing DataJoint generation logic
        # from your dj/tracker.py file
        pass


# Utility functions
def validate_pipeline_serializability(pipeline: Pipeline) -> None:
    """Helper to check if pipeline can be saved"""
    try:
        pipeline._validate_serializable()
        print("✅ Pipeline is ready for serialization")

        # Additional performance warnings
        stats = pipeline.get_stats()
        if stats["total_invocations"] > 1000:
            print(
                f"⚠️  Large pipeline ({stats['total_invocations']} invocations) - views may be slow to build"
            )

    except SerializationError as e:
        print("❌ Pipeline cannot be serialized:")
        print(str(e))
        print("\n💡 Convert lambda functions to named functions:")
        print("    lambda x: x > 0.8  →  def filter_func(x): return x > 0.8")


def create_example_pipeline() -> Pipeline:
    """Create an example pipeline for testing"""
    from orcabridge import GlobSource, function_pod

    @function_pod
    def example_function(input_file):
        return f"processed_{input_file}"

    pipeline = Pipeline("example")

    with pipeline:
        # This would need actual operations to be meaningful
        # source = GlobSource('data', './test_data', '*.txt')()
        # results = source >> example_function
        pass

    return pipeline
