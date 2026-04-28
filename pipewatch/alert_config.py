"""Load alert rules from a simple JSON or dict configuration."""

import json
from typing import List, Dict, Any
from pipewatch.alerts import AlertRule


DEFAULT_RULES: List[Dict[str, Any]] = [
    {
        "name": "low-success-rate",
        "pipeline": "*",
        "metric": "success_rate",
        "threshold": 0.8,
        "comparator": "lt",
        "message": "Success rate dropped below 80%",
    },
]


def _expand_wildcard(rule_dict: Dict[str, Any], pipeline_names: List[str]) -> List[Dict[str, Any]]:
    """Expand a wildcard pipeline rule into one rule per known pipeline."""
    if rule_dict.get("pipeline") != "*":
        return [rule_dict]
    expanded = []
    for name in pipeline_names:
        copy = dict(rule_dict)
        copy["pipeline"] = name
        copy["name"] = f"{rule_dict['name']}:{name}"
        expanded.append(copy)
    return expanded


def rules_from_dict(
    config: List[Dict[str, Any]],
    pipeline_names: List[str] = None,
) -> List[AlertRule]:
    """Parse a list of rule dicts into AlertRule objects.

    Wildcarded pipeline entries are expanded using *pipeline_names* when provided.
    """
    pipeline_names = pipeline_names or []
    result: List[AlertRule] = []
    for entry in config:
        expanded = _expand_wildcard(entry, pipeline_names)
        for rule_dict in expanded:
            result.append(
                AlertRule(
                    name=rule_dict["name"],
                    pipeline=rule_dict["pipeline"],
                    metric=rule_dict["metric"],
                    threshold=float(rule_dict["threshold"]),
                    comparator=rule_dict["comparator"],
                    message=rule_dict.get("message", ""),
                )
            )
    return result


def rules_from_json(json_str: str, pipeline_names: List[str] = None) -> List[AlertRule]:
    """Parse alert rules from a JSON string."""
    config = json.loads(json_str)
    return rules_from_dict(config, pipeline_names)


def default_rules(pipeline_names: List[str] = None) -> List[AlertRule]:
    """Return the built-in default alert rules, optionally expanded for known pipelines."""
    return rules_from_dict(DEFAULT_RULES, pipeline_names or [])
