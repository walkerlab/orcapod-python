from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable
from orcapod.types import DataValue


@runtime_checkable
class ExecutionEngine(Protocol):
    @property
    def name(self) -> str: ...

    def submit_sync(self, function: Callable, *args, **kwargs) -> Any:
        """
        Run the given function with the provided arguments.
        This method should be implemented by the execution engine.
        """
        ...

    async def submit_async(self, function: Callable, *args, **kwargs) -> Any:
        """
        Asynchronously run the given function with the provided arguments.
        This method should be implemented by the execution engine.
        """
        ...

    # TODO: consider adding batch submission


@runtime_checkable
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


@runtime_checkable
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
    def label(self) -> str:
        """
        Return the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Returns:
            str: Human-readable label for this object
            None: No label is set (will use default naming)
        """
        ...

    @label.setter
    def label(self, label: str | None) -> None:
        """
        Set the human-readable label for this object.

        Labels should be descriptive and help users understand the purpose
        or role of the object in the computational graph.

        Args:
            value (str): Human-readable label for this object
        """
        ...
