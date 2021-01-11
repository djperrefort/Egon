"""Tests the validation processes of pipelione nodes"""

from unittest import TestCase, skip

from egon import exceptions
from egon.connectors import Input, Output
from egon.nodes import Node, Source, Target


class BaseTests:
    """Generic tests applicable to multiple Node subclasses"""

    node_type = None  # The Node subclass type to test
    malformed_exception = None  # Validation error expected if the Node is malformed

    def setUp(self) -> None:
        """Create a subclass of the type being tested that implements abstract methods"""

        class TestSource(self.node_type):
            def action(self) -> None:
                """Dummy placeholder method required by abstract parent"""

        self.test_class = TestSource()

    def test_error_if_orphaned(self) -> None:
        """Test ``OrphanedNodeError`` is raised for a Node with no connectors"""

        with self.assertRaises(exceptions.OrphanedNodeError):
            self.test_class.validate()

    def test_error_if_malformed(self) -> None:
        """Test a malformed error is raised"""

        self.test_class.input = Input()
        self.test_class.output = Output()
        with self.assertRaises(self.malformed_exception):
            self.test_class.validate()


class SourceValidation(BaseTests, TestCase):
    """Test the validation of ``Source`` nodes"""

    node_type = Source
    malformed_exception = exceptions.MalformedSourceError


class TargetValidation(BaseTests, TestCase):
    """Test the validation of ``Target`` nodes"""

    node_type = Target
    malformed_exception = exceptions.MalformedTargetError


class InlineValidation(BaseTests, TestCase):
    """Test the validation of ``Inline`` nodes"""

    node_type = Node

    @skip('Inline nodes cannot be malformed')
    def test_error_if_malformed(self) -> None:
        """Inline nodes cannot be malformed"""
