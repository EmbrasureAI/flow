"""Bounded parsing for exact external-reconciliation acknowledgements."""

import json


MAX_LOG_LINE_BYTES = 1 << 20


def scan_reconciliation_events(path, offset, expected):
    """Return the next complete-line cursor and matching acknowledgements."""
    acknowledged = {}
    with path.open("rb") as source:
        source.seek(offset)
        while True:
            line_start = source.tell()
            line = source.readline(MAX_LOG_LINE_BYTES + 1)
            if not line:
                break
            if len(line) > MAX_LOG_LINE_BYTES:
                raise ValueError(f"oversized daemon log line at byte {line_start}")
            if not line.endswith(b"\n"):
                break
            offset = source.tell()
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(record, dict):
                continue
            fields = record.get("fields")
            if not isinstance(fields, dict) or fields.get("event") != "external_snapshot_reconciled":
                continue
            identity = (fields.get("table_id"), fields.get("snapshot_id"))
            if identity in expected:
                acknowledged[identity] = {
                    "table_id": identity[0],
                    "snapshot_id": identity[1],
                    "sequence_number": fields.get("sequence_number"),
                    "reconciliation_kind": fields.get("reconciliation_kind"),
                }
    return offset, acknowledged
