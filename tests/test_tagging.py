"""Tests for pipewatch.tagging and pipewatch.tagging_config."""
from __future__ import annotations

import json
import pytest

from pipewatch.tagging import TagRegistry, registry_from_dict
from pipewatch.tagging_config import (
    default_registry,
    merge_registries,
    tagging_registry_from_json,
)


# ---------------------------------------------------------------------------
# TagRegistry unit tests
# ---------------------------------------------------------------------------

class TestTagRegistry:
    def test_register_and_retrieve(self):
        reg = TagRegistry()
        reg.register("ingest", ["env:prod", "team:data"])
        assert reg.tags_for("ingest") == ["env:prod", "team:data"]

    def test_tags_for_unknown_pipeline_returns_empty(self):
        reg = TagRegistry()
        assert reg.tags_for("nonexistent") == []

    def test_duplicate_tags_not_added_twice(self):
        reg = TagRegistry()
        reg.register("ingest", ["env:prod"])
        reg.register("ingest", ["env:prod", "team:data"])
        assert reg.tags_for("ingest").count("env:prod") == 1

    def test_tags_are_lowercased(self):
        reg = TagRegistry()
        reg.register("pipe", ["ENV:PROD"])
        assert "env:prod" in reg.tags_for("pipe")

    def test_pipelines_with_tag(self):
        reg = TagRegistry()
        reg.register("a", ["env:prod"])
        reg.register("b", ["env:staging"])
        reg.register("c", ["env:prod", "team:bi"])
        result = reg.pipelines_with_tag("env:prod")
        assert sorted(result) == ["a", "c"]

    def test_filter_by_tag_exact(self):
        reg = TagRegistry()
        reg.register("x", ["team:data"])
        reg.register("y", ["team:bi"])
        reg.register("z", ["team:data", "env:prod"])
        result = reg.filter_by_tag(["x", "y", "z"], "team:data")
        assert sorted(result) == ["x", "z"]

    def test_filter_by_tag_wildcard(self):
        reg = TagRegistry()
        reg.register("p1", ["env:prod"])
        reg.register("p2", ["env:staging"])
        reg.register("p3", ["team:data"])
        result = reg.filter_by_tag(["p1", "p2", "p3"], "env:*")
        assert sorted(result) == ["p1", "p2"]

    def test_filter_by_tag_no_match(self):
        reg = TagRegistry()
        reg.register("p1", ["team:data"])
        result = reg.filter_by_tag(["p1"], "env:prod")
        assert result == []

    def test_all_tags_sorted_and_unique(self):
        reg = TagRegistry()
        reg.register("a", ["z-tag", "a-tag"])
        reg.register("b", ["a-tag", "m-tag"])
        assert reg.all_tags() == ["a-tag", "m-tag", "z-tag"]


# ---------------------------------------------------------------------------
# registry_from_dict
# ---------------------------------------------------------------------------

class TestRegistryFromDict:
    def test_empty_dict_returns_empty_registry(self):
        reg = registry_from_dict({})
        assert reg.all_tags() == []

    def test_pipelines_key_parsed(self):
        data = {"pipelines": {"ingest": ["env:prod"], "etl": ["env:staging"]}}
        reg = registry_from_dict(data)
        assert reg.tags_for("ingest") == ["env:prod"]
        assert reg.tags_for("etl") == ["env:staging"]

    def test_non_list_tags_ignored(self):
        data = {"pipelines": {"broken": "not-a-list"}}
        reg = registry_from_dict(data)
        assert reg.tags_for("broken") == []


# ---------------------------------------------------------------------------
# tagging_config helpers
# ---------------------------------------------------------------------------

class TestTaggingConfig:
    def test_default_registry_is_empty(self):
        reg = default_registry()
        assert reg.all_tags() == []

    def test_from_json_file(self, tmp_path):
        data = {"pipelines": {"reports": ["team:bi", "env:prod"]}}
        f = tmp_path / "tags.json"
        f.write_text(json.dumps(data), encoding="utf-8")
        reg = tagging_registry_from_json(f)
        assert "team:bi" in reg.tags_for("reports")

    def test_from_json_string(self):
        raw = json.dumps({"pipelines": {"pipe": ["env:dev"]}})
        reg = tagging_registry_from_json(raw)
        assert reg.tags_for("pipe") == ["env:dev"]

    def test_merge_registries(self):
        r1 = TagRegistry()
        r1.register("a", ["tag1"])
        r2 = TagRegistry()
        r2.register("a", ["tag2"])
        r2.register("b", ["tag3"])
        merged = merge_registries(r1, r2)
        assert sorted(merged.tags_for("a")) == ["tag1", "tag2"]
        assert merged.tags_for("b") == ["tag3"]
