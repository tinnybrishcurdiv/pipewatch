"""Pipeline tagging — attach metadata tags to pipelines and filter/group by them."""
from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch
from typing import Dict, Iterable, List, Optional


@dataclass
class TagRegistry:
    """Holds pipeline -> tag mappings loaded from config."""

    _tags: Dict[str, List[str]] = field(default_factory=dict)

    def register(self, pipeline: str, tags: Iterable[str]) -> None:
        """Attach *tags* to *pipeline*, merging with any existing tags."""
        existing = self._tags.setdefault(pipeline, [])
        for t in tags:
            t = t.strip().lower()
            if t and t not in existing:
                existing.append(t)

    def tags_for(self, pipeline: str) -> List[str]:
        """Return tags registered for *pipeline* (empty list if none)."""
        return list(self._tags.get(pipeline, []))

    def pipelines_with_tag(self, tag: str) -> List[str]:
        """Return all pipeline names that carry *tag* (exact, case-insensitive)."""
        tag = tag.strip().lower()
        return [p for p, tags in self._tags.items() if tag in tags]

    def filter_by_tag(
        self,
        pipelines: Iterable[str],
        tag_pattern: str,
    ) -> List[str]:
        """Return pipelines whose tag list contains at least one tag matching *tag_pattern*.

        *tag_pattern* supports ``*`` and ``?`` wildcards via :func:`fnmatch`.
        """
        pattern = tag_pattern.strip().lower()
        result: List[str] = []
        for p in pipelines:
            if any(fnmatch(t, pattern) for t in self._tags.get(p, [])):
                result.append(p)
        return result

    def all_tags(self) -> List[str]:
        """Return a sorted, deduplicated list of every tag in the registry."""
        seen: set[str] = set()
        for tags in self._tags.values():
            seen.update(tags)
        return sorted(seen)


def registry_from_dict(data: dict) -> TagRegistry:
    """Build a :class:`TagRegistry` from a plain dict.

    Expected shape::

        {
            "pipelines": {
                "ingest": ["team:data", "env:prod"],
                "etl-*":  ["env:staging"]
            }
        }

    Keys under ``pipelines`` may contain glob patterns; every matching
    pipeline name (if already known) will receive those tags.  Unknown
    names are stored verbatim so callers can register them later.
    """
    registry = TagRegistry()
    for pipeline, tags in data.get("pipelines", {}).items():
        if isinstance(tags, list):
            registry.register(pipeline, tags)
    return registry
