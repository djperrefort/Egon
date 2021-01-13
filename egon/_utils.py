from typing import Any, Collection, Iterable, Optional


class KillSignal:
    """Used to indicate that a process should exit"""


class ObjectCollection:
    """Collection of objects with O(1) add and remove"""

    def __init__(self, data: Optional[Collection] = None) -> None:
        """A mutable collection of arbitrary objects

        Args:
            data: Populate the collection instance with the given data
        """

        # Map object hash values to their index in a list
        self._object_list = list(set(data)) if data else []
        self._index_map = {o: i for i, o in enumerate(self._object_list)}

    def add(self, x: Any) -> None:
        """Add a hashable object to the collection

        Args:
            x: The object to add
        """

        # Exit if ``x`` is already in the collection
        if x in self._index_map:
            return

        # Add ``x`` to the end of the collection
        self._index_map[x] = len(self._object_list)
        self._object_list.append(x)

    def remove(self, x: Any) -> None:
        """Remove an object from the collection

        Args:
            x: The object to remove
        """

        index = self._index_map[x]

        # Swap element with last element so that removal from the list can be done in O(1) time
        size = len(self._object_list)
        last = self._object_list[size - 1]
        self._object_list[index], self._object_list[size - 1] = self._object_list[size - 1], self._object_list[index]

        # Update hash table for new index of last element
        self._index_map[last] = index

        del self._index_map[x]
        del self._object_list[-1]

    def __iter__(self) -> Iterable:
        return iter(self._object_list)

    def __contains__(self, item: Any) -> bool:
        return item in self._object_list

    def __len__(self) -> int:
        return len(self._object_list)

    def __repr__(self) -> str:  # pragma: no cover
        return f'<Container({self._object_list})>'
