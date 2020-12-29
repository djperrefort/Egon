"""Tests the creation of pipeline nodes using function decorators."""

import inspect
from typing import Type
from unittest import TestCase, skip

from egon.decorators import as_node, as_source, as_target
from egon.nodes import Node, Source, Target


def function_add(x: int, y: int) -> int:
    """Return the sum of two numbers"""

    return x + y


class BaseTests:
    """Generic tests applicable to multiple function decorators"""

    wrapper: callable  # The function decorator to test
    return_type: Type  # The expected return type of the decorator

    @classmethod
    def setUpClass(cls) -> None:
        """Wrap the ``function_add`` function with a decorator"""

        cls.wrapped_function = cls.wrapper(function_add)

    def test_wrapped_return_type(self) -> None:
        """Test the function decorator returns the expected object type"""

        self.assertIsInstance(self.wrapped_function, self.return_type)

    def test_wrapped_is_still_callable(self) -> None:
        """Test wrapped functions are still callable"""

        test_args = 1, 2
        expected_return = function_add(*test_args)
        wrapped_return = self.wrapped_function(*test_args)
        self.assertEqual(expected_return, wrapped_return)

    def test_wrapped_function_maintains_signature(self) -> None:
        """Test the wrapped function has the same signature as the original function"""

        original_sig = inspect.getfullargspec(function_add)
        wrapped_sig = inspect.getfullargspec(self.wrapped_function)
        self.assertEqual(wrapped_sig, original_sig)


class WrappedAsSource(BaseTests, TestCase):
    """Tests for the ``as_source`` decorator"""

    wrapper = staticmethod(as_source)
    return_type = Source

    @skip
    def test_wrapped_is_still_callable(self) -> None:
        """Test wrapped functions are still callable"""

    def test_wrapped_is_still_generator(self) -> None:
        """Test the wrapped function still acts as a generator"""

        def generator():
            for i in range(10):
                yield i

        wrapped = self.wrapper(generator)
        self.assertListEqual(list(generator()), list(wrapped()))


class WrappedAsNode(BaseTests, TestCase):
    """Tests for the ``as_node`` decorator"""

    wrapper = staticmethod(as_node)
    return_type = Node


class WrappedAsTarget(BaseTests, TestCase):
    """Tests for the ``as_target`` decorator"""

    wrapper = staticmethod(as_target)
    return_type = Target
