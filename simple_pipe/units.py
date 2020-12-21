import abc
import inspect
from typing import Any, List

from . import connectors


class AbstractPipelineUnit:
    """Base class for constructing pipeline units"""

    def __init__(self) -> None:
        """Represents a single pipeline processing unit"""

        self.validate()
        for source in self.get_inputs():
            setattr(self, f'source_{source}', source.connect)

        for target in self.get_outputs():
            setattr(self, f'target_{target}', target.connect)

        self.__call__ = self.action

    def get_inputs(self) -> List[connectors.Input]:
        """Returns a list of input connections assigned to this instance

        Returns:
            A list of ``Input`` connection objects
        """

        predicate = lambda a: isinstance(a, connectors.Input)
        return [getattr(self, a[0]) for a in inspect.getmembers(self, predicate)]

    def get_outputs(self) -> List[connectors.Output]:
        """Returns a list of output connections assigned to this instance

        Returns:
            A list of ``Output`` connection objects
        """

        predicate = lambda a: isinstance(a, connectors.Output)
        return [getattr(self, a[0]) for a in inspect.getmembers(self, predicate)]

    @abc.abstractmethod
    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        pass

    def setup(self) -> None:
        """Setup tasks called before running ``action``"""

        pass

    @abc.abstractmethod
    def action(self, *args: Any, **kwargs: Any) -> Any:
        """The analysis task performed by the parent pipeline process"""

        pass

    def teardown(self) -> None:
        """Teardown tasks called after running ``action``"""

        pass


class Source(AbstractPipelineUnit, metaclass=abc.ABCMeta):
    """A pipeline process that only has output streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if self.get_inputs():
            raise ValueError('Source objects cannot have upstream components')


class Target(AbstractPipelineUnit, metaclass=abc.ABCMeta):
    """A pipeline process that only has input streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        if self.get_outputs():
            raise ValueError('Source objects cannot have upstream components')


class Inline(Source, Target, metaclass=abc.ABCMeta):
    """A pipeline process that can have any number of input or output streams"""

    def validate(self) -> None:
        """Raise exception if the object is not a valid instance

        Raises:
            ValueError: For an invalid instance construction
        """

        pass
