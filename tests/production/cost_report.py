#!/usr/bin/env python3
"""Calculate scoped costs from explicit measurements and prices, without service access."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sys


QUANTITIES = {
    "committed_mutations": "mutations",
    "source_bytes": "bytes",
    "active_table_seconds": "table-seconds",
    "object_store_requests": "requests",
    "catalog_commits": "commits",
    "compaction_bytes": "bytes",
}
COST_UNITS = {"requests", "hours", "vCPU-hours", "GiB-hours", "GiB", "bytes", "seconds"}
INTEGRAL_UNITS = {"mutations", "bytes", "requests", "commits", "calls"}


def keys(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= value.keys() or value.keys() - set(required) - set(optional):
        raise ValueError(f"expected fields {sorted(required)}; optional {sorted(optional)}")


def text(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("scope, basis and evidence must be nonempty strings")
    return value


def number(value, integral=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("quantities and prices must be numeric")
    if integral and not isinstance(value, int):
        raise ValueError("counts and bytes must be JSON integers")
    if value < 0 or not math.isfinite(value):
        raise ValueError("quantities and prices must be finite and nonnegative")
    return Decimal(str(value))


def window(value):
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("window must be [UTC start, UTC end]")
    parsed = [datetime.fromisoformat(text(item).replace("Z", "+00:00")) for item in value]
    if any(item.utcoffset() != timezone.utc.utcoffset(item) for item in parsed) or parsed[1] <= parsed[0]:
        raise ValueError("window must have increasing, timezone-aware UTC endpoints")
    return parsed


def read_json(path):
    raw = path.read_bytes()
    return json.loads(raw), {"path": str(path.resolve()), "sha256": hashlib.sha256(raw).hexdigest()}


def report(measurements_path, prices_path):
    measured, measurement_identity = read_json(measurements_path)
    prices, price_identity = read_json(prices_path)
    keys(measured, {"accounting_scope", "window", "quantities", "cost_items"}, {"fileio_calls"})
    scope = text(measured["accounting_scope"])
    period = window(measured["window"])
    keys(measured["quantities"], QUANTITIES)
    if not isinstance(measured["cost_items"], dict) or not measured["cost_items"]:
        raise ValueError("cost_items must explicitly list the nonempty accounting scope, using null for unknown usage")
    if not isinstance(prices, dict) or prices.keys() - measured["cost_items"].keys():
        raise ValueError("every priced item must be declared in cost_items")
    evidence = {}

    def quantity(value, unit=None):
        if value is None:
            return None
        keys(value, {"value", "unit", "window", "evidence", "basis"})
        text(value["basis"])
        if window(value["window"]) != period:
            raise ValueError("quantity window differs from the accounting window")
        if (unit is not None and value["unit"] != unit) or (unit is None and value["unit"] not in COST_UNITS):
            raise ValueError("quantity unit is invalid; FileIO calls cannot be priced as provider requests")
        amount = number(value["value"], value["unit"] in INTEGRAL_UNITS)
        path = (measurements_path.parent / text(value["evidence"])).resolve()
        if str(path) not in evidence:
            with path.open("rb") as source:
                evidence[str(path)] = hashlib.file_digest(source, "sha256").hexdigest()
        return amount

    quantities = {name: quantity(measured["quantities"][name], unit) for name, unit in QUANTITIES.items()}
    fileio = quantity(measured.get("fileio_calls"), "calls")
    breakdown, missing = {}, []
    subtotal = Decimal(0)
    for name, usage in measured["cost_items"].items():
        text(name)
        amount = quantity(usage)
        price = prices.get(name)
        rate = None
        if price is not None:
            keys(price, {"usd_per_unit", "unit"})
            if price["unit"] not in COST_UNITS or usage is not None and price["unit"] != usage["unit"]:
                raise ValueError(f"{name}: price unit differs from usage unit")
            rate = number(price["usd_per_unit"])
        reason = "usage is unmeasured" if amount is None else "price is unspecified" if rate is None else None
        cost = None if reason else amount * rate
        if cost is None:
            missing.append(name)
        else:
            subtotal += cost
        breakdown[name] = {"usd": cost, "reason": reason}
    total = None if missing else subtotal

    def ratio(numerator, denominator, scale, unit, missing_numerator):
        if numerator is None:
            return {"value": None, "unit": unit, "reason": missing_numerator}
        if quantities[denominator] is None:
            return {"value": None, "unit": unit, "reason": f"{denominator} is unmeasured"}
        if quantities[denominator] == 0:
            return {"value": None, "unit": unit, "reason": f"{denominator} is zero"}
        return {"value": numerator * scale / quantities[denominator], "unit": unit}

    incomplete = "declared cost scope is incomplete: " + ", ".join(missing)
    metrics = {
        "dollars_per_million_mutations": ratio(total, "committed_mutations", 1_000_000,
                                               "USD / million mutations", incomplete),
        "dollars_per_GiB_ingested": ratio(total, "source_bytes", 1 << 30, "USD / GiB", incomplete),
        "dollars_per_active_table_hour": ratio(total, "active_table_seconds", 3600,
                                                "USD / active-table-hour", incomplete),
        "object_store_operations_per_million_mutations": ratio(quantities["object_store_requests"],
            "committed_mutations", 1_000_000, "provider requests / million mutations", "object_store_requests is unmeasured"),
        "catalog_commits_per_million_mutations": ratio(quantities["catalog_commits"], "committed_mutations",
            1_000_000, "commits / million mutations", "catalog_commits is unmeasured"),
        "compaction_bytes_per_source_byte": ratio(quantities["compaction_bytes"], "source_bytes", 1,
            "bytes / source byte", "compaction_bytes is unmeasured"),
    }
    return {
        "accounting_scope": scope,
        "window": measured["window"],
        "inputs": {"measurements": measurement_identity, "prices": price_identity, "evidence_sha256": evidence},
        "measurements": measured,
        "prices": prices,
        "known_subtotal_usd": subtotal,
        "total_usd_within_declared_scope": total,
        "missing_cost_items": missing,
        "cost_breakdown": breakdown,
        "metrics": metrics,
        "fileio_diagnostic": ratio(fileio, "committed_mutations", 1_000_000, "FileIO calls / million mutations",
                                   "fileio_calls is unmeasured"),
        "limits": ["Completeness refers only to the user-declared accounting scope, not an entire provider bill.",
                   "Evidence files are hashed, not interpreted; the supplied measurements and their scope remain the user's responsibility.",
                   "FileIO calls and accepted bytes do not measure provider requests, wire traffic or billed storage.",
                   "This report neither changes benchmark results nor qualifies latency or throughput."],
    }


def decimal_json(value):
    if isinstance(value, Decimal):
        result = float(value)
        if math.isfinite(result):
            return result
        raise ValueError("calculated result exceeds finite JSON numeric range")
    raise TypeError(f"cannot encode {type(value).__name__}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--measurements", type=Path, required=True)
    parser.add_argument("--prices", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = report(args.measurements, args.prices)
        encoded = json.dumps(result, indent=2, default=decimal_json, allow_nan=False) + "\n"
        with args.output.open("x") as output:
            output.write(encoded)
    except (OSError, ValueError, TypeError, OverflowError) as error:
        print(f"cost report: {error}", file=sys.stderr)
        return 2
    print(f"Wrote {args.output.resolve()} (declared scope {'incomplete' if result['missing_cost_items'] else 'complete'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
