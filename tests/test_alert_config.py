"""Tests for pipewatch.alert_config — rule loading from dict/JSON and defaults."""

import json
import pytest

from pipewatch.alert_config import (
    _expand_wildcard,
    rules_from_dict,
    rules_from_json,
    default_rules,
)
from pipewatch.alerts import AlertRule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PIPELINES = ["ingest", "transform", "export"]


def _minimal_rule_dict(pipeline="ingest", metric="success_rate", comparator="<", threshold=0.9):
    return {
        "pipeline": pipeline,
        "metric": metric,
        "comparator": comparator,
        "threshold": threshold,
    }


# ---------------------------------------------------------------------------
# _expand_wildcard
# ---------------------------------------------------------------------------


class TestExpandWildcard:
    def test_wildcard_expands_to_all_pipelines(self):
        raw = [_minimal_rule_dict(pipeline="*")]
        expanded = _expand_wildcard(raw, PIPELINES)
        assert len(expanded) == len(PIPELINES)
        names = {r["pipeline"] for r in expanded}
        assert names == set(PIPELINES)

    def test_non_wildcard_passes_through_unchanged(self):
        raw = [_minimal_rule_dict(pipeline="ingest")]
        expanded = _expand_wildcard(raw, PIPELINES)
        assert len(expanded) == 1
        assert expanded[0]["pipeline"] == "ingest"

    def test_mixed_wildcard_and_specific(self):
        raw = [
            _minimal_rule_dict(pipeline="*", metric="success_rate"),
            _minimal_rule_dict(pipeline="ingest", metric="error_rate"),
        ]
        expanded = _expand_wildcard(raw, PIPELINES)
        # 3 from wildcard + 1 specific
        assert len(expanded) == 4

    def test_empty_pipelines_list_with_wildcard_returns_empty(self):
        raw = [_minimal_rule_dict(pipeline="*")]
        expanded = _expand_wildcard(raw, [])
        assert expanded == []

    def test_no_pipelines_needed_when_no_wildcard(self):
        raw = [_minimal_rule_dict(pipeline="ingest")]
        expanded = _expand_wildcard(raw, [])  # empty pipeline list is fine
        assert len(expanded) == 1


# ---------------------------------------------------------------------------
# rules_from_dict
# ---------------------------------------------------------------------------


class TestRulesFromDict:
    def test_returns_list_of_alert_rules(self):
        cfg = {"rules": [_minimal_rule_dict()]}
        rules = rules_from_dict(cfg, known_pipelines=PIPELINES)
        assert all(isinstance(r, AlertRule) for r in rules)

    def test_correct_number_of_rules(self):
        cfg = {
            "rules": [
                _minimal_rule_dict(pipeline="ingest"),
                _minimal_rule_dict(pipeline="transform", metric="error_rate", comparator=">", threshold=0.1),
            ]
        }
        rules = rules_from_dict(cfg, known_pipelines=PIPELINES)
        assert len(rules) == 2

    def test_wildcard_expansion_via_rules_from_dict(self):
        cfg = {"rules": [_minimal_rule_dict(pipeline="*")]}
        rules = rules_from_dict(cfg, known_pipelines=PIPELINES)
        assert len(rules) == len(PIPELINES)

    def test_missing_rules_key_returns_empty(self):
        rules = rules_from_dict({}, known_pipelines=PIPELINES)
        assert rules == []

    def test_rule_fields_are_set_correctly(self):
        cfg = {"rules": [_minimal_rule_dict(pipeline="ingest", metric="success_rate", comparator="<", threshold=0.95)]}
        rule = rules_from_dict(cfg, known_pipelines=PIPELINES)[0]
        assert rule.pipeline == "ingest"
        assert rule.metric == "success_rate"
        assert rule.comparator == "<"
        assert rule.threshold == pytest.approx(0.95)

    def test_invalid_comparator_raises(self):
        cfg = {"rules": [_minimal_rule_dict(comparator="!=")]}
        with pytest.raises(ValueError):
            rules_from_dict(cfg, known_pipelines=PIPELINES)


# ---------------------------------------------------------------------------
# rules_from_json
# ---------------------------------------------------------------------------


class TestRulesFromJson:
    def test_parses_valid_json_string(self):
        payload = json.dumps({"rules": [_minimal_rule_dict()]})
        rules = rules_from_json(payload, known_pipelines=PIPELINES)
        assert len(rules) == 1
        assert isinstance(rules[0], AlertRule)

    def test_invalid_json_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid JSON"):
            rules_from_json("{not valid json", known_pipelines=PIPELINES)


# ---------------------------------------------------------------------------
# default_rules
# ---------------------------------------------------------------------------


class TestDefaultRules:
    def test_returns_list(self):
        rules = default_rules(PIPELINES)
        assert isinstance(rules, list)

    def test_returns_alert_rule_instances(self):
        rules = default_rules(PIPELINES)
        assert all(isinstance(r, AlertRule) for r in rules)

    def test_covers_all_pipelines(self):
        rules = default_rules(PIPELINES)
        covered = {r.pipeline for r in rules}
        assert covered == set(PIPELINES)

    def test_empty_pipelines_returns_empty(self):
        rules = default_rules([])
        assert rules == []
