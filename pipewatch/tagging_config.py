"""Load tagging configuration from dict / JSON."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Union

from pipewatch.tagging import TagRegistry, registry_from_dict


def tagging_registry_from_json(source: Union[str, Path]) -> TagRegistry:
    """Parse *source* (a JSON file path or a raw JSON string) into a :class:`TagRegistry`.

    The JSON must follow the shape expected by :func:`registry_from_dict`::

        {
          "pipelines": {
            "ingest":  ["team:data", "env:prod"],
            "reports": ["team:bi"]
          }
        }
    """
    path = Path(source)
    if path.exists():
        raw = path.read_text(encoding="utf-8")
    else:
        raw = str(source)
    return registry_from_dict(json.loads(raw))


def default_registry() -> TagRegistry:
    """Return an empty :class:`TagRegistry` suitable for use when no config is provided."""
    return TagRegistry()


def merge_registries(*registries: TagRegistry) -> TagRegistry:
    """Merge multiple registries into one, combining tags for shared pipeline names."""
    merged = TagRegistry()
    for reg in registries:
        # pylint: disable=protected-access
        for pipeline, tags in reg._tags.items():
            merged.register(pipeline, tags)
    return merged
