"""Export pipeline metrics snapshots to JSON or CSV formats."""

import csv
import json
import io
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.metrics import PipelineMetrics, to_dict


def _timestamp_now() -> str:
    """Return current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def export_json(
    snapshots: List[PipelineMetrics],
    pipeline_name: str,
    exported_at: Optional[str] = None,
) -> str:
    """Serialize a list of PipelineMetrics to a JSON string.

    Args:
        snapshots: Metrics objects to export.
        pipeline_name: Logical name of the pipeline being exported.
        exported_at: Optional ISO timestamp; defaults to current UTC time.

    Returns:
        Pretty-printed JSON string.
    """
    payload = {
        "pipeline": pipeline_name,
        "exported_at": exported_at or _timestamp_now(),
        "records": [to_dict(m) for m in snapshots],
    }
    return json.dumps(payload, indent=2)


def export_csv(
    snapshots: List[PipelineMetrics],
    pipeline_name: str,
) -> str:
    """Serialize a list of PipelineMetrics to a CSV string.

    Args:
        snapshots: Metrics objects to export.
        pipeline_name: Logical name of the pipeline being exported.

    Returns:
        CSV-formatted string with a header row.
    """
    if not snapshots:
        return ""

    output = io.StringIO()
    rows = [to_dict(m) for m in snapshots]
    # Inject pipeline column
    for row in rows:
        row["pipeline"] = pipeline_name

    fieldnames = ["pipeline"] + [k for k in rows[0] if k != "pipeline"]
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
