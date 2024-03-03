"""Utility functions for the psmq package."""
from itertools import islice
from typing import Any, Iterable, Tuple


def list_to_dict(kv_list: list) -> dict:
    """Convert a list of alternating key-value pairs into a dict."""

    def batched(iterable: Iterable) -> Iterable[Tuple[Any, Any]]:
        it = iter(iterable)
        while batch := tuple(islice(it, 2)):
            yield batch  # type: ignore[misc]

    r_str = []
    for item in kv_list:
        try:
            r_str.append(item.decode("utf8") if isinstance(item, bytes) else item)
        except UnicodeDecodeError:
            r_str.append(item)
    return dict(batched(r_str))
