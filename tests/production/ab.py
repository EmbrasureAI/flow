#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Build two revisions and compare them with interleaved benchmark runs.

    tests/production/ab.py --base origin/main --candidate . --runs 2
    tests/production/ab.py --base HEAD --candidate HEAD \\
        --candidate-args "--limit collapse_memory_bytes=33554432"

`.` is this working tree, including uncommitted changes; any other value is a
git revision built from a detached worktree under target/ab/trees. Identical
sources build once, so the same binary can compare configuration flags.

Builds use the `bench-fast` profile by default: no LTO and incremental, so a
one-file change rebuilds in seconds. They link jemalloc, like the service
integration builds; pass `--features ""` for the system allocator. Use it to rank
changes; confirm a result with `--profile release` (or `profiling`) before
publishing numbers.

Runs alternate base and candidate (B C, C B, ...) so host drift affects both.
Every run executes this checkout's benchmark harness, verifies every row and
writes its usual report. The summary compares medians; CDC service time per
mutation is the low-noise measure. Start the fixture first with
`tests/production/fixture.sh up`. Do not run other benchmarks at the same time.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from epoch_report import compare, summarize, table  # noqa: E402
from freeze_build import freeze  # noqa: E402
from provenance import ROOT, repository_identity  # noqa: E402

AB = ROOT / "target" / "ab"
# Outside the checkout so every worktree and checkout on the machine shares it.
CARGO = Path(os.environ.get("FLOW_AB_CARGO_DIR", Path.home() / ".cache" / "flow-ab" / "cargo"))
DEFAULT_ARGS = "--rate 30000 --duration 30 --warmup 5"
# The service integration builds and the jemalloc benchmark reports use it too.
DEFAULT_FEATURES = "flow-daemon/jemalloc"
KEEP_STATE = {"metrics.prom", "status.json"}


def git(*args, cwd=ROOT):
    return subprocess.check_output(["git", *args], cwd=cwd, text=True).strip()


def source_tree(revision):
    if revision == ".":
        return ROOT
    commit = git("rev-parse", "--verify", f"{revision}^{{commit}}")
    tree = AB / "trees" / commit[:12]
    if tree.exists():
        # A tree whose git metadata was removed (for example by copying the
        # repository over it) is unusable; recreate it.
        check = subprocess.run(["git", "rev-parse", "HEAD"], cwd=tree, capture_output=True, text=True)
        if check.returncode != 0 or check.stdout.strip() != commit:
            shutil.rmtree(tree)
    if not tree.exists():
        tree.parent.mkdir(parents=True, exist_ok=True)
        git("worktree", "prune")  # forget trees deleted by hand
        git("worktree", "add", "--detach", str(tree), commit)
    return tree


def build(tree, profile, features):
    identity = repository_identity(tree)
    label = identity["head"][:12]
    if identity["dirty"]:
        label += "-dirty-" + identity["dirty_diff_sha256"][:12]
    suffix = "".join(f"-{feature.split('/')[-1]}" for feature in features)
    output = AB / "builds" / f"{label}-{profile}{suffix}"
    manifest = output / "build-manifest.json"
    if manifest.exists():
        if json.loads(manifest.read_text())["source"] == identity:
            print(f"reusing {output}", flush=True)
            return output
        shutil.rmtree(output)
    print(f"building {tree} ({label}, {profile})", flush=True)
    freeze(output, profile=profile, features=features, source=tree, target_dir=CARGO / profile)
    return output


def fixture(command):
    subprocess.run([str(ROOT / "tests/production/fixture.sh"), command], check=True)


def ports():
    values = {"FLOW_PRODUCTION_PG_PORT": "55432", "FLOW_PRODUCTION_S3_PORT": "59000",
              "FLOW_PRODUCTION_REST_PORT": "58181"}
    path = ROOT / "target" / "production-local" / "ports.env"
    if path.exists():
        for line in path.read_text().splitlines():
            key, _, value = line.partition("=")
            if key.strip() in values and value.strip():
                values[key.strip()] = value.strip()
    return values


def run(binaries, artifacts, arguments):
    port = ports()
    environment = os.environ | {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "demo-access"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "demo-secret-key"),
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "FLOW_POSTGRES_URL": os.environ.get(
            "FLOW_POSTGRES_URL",
            f"postgres://flow:local-test-password@127.0.0.1:{port['FLOW_PRODUCTION_PG_PORT']}/flow?sslmode=disable"),
    }
    command = ["uv", "run", str(ROOT / "tests/production/benchmark.py"),
               "--catalog-uri", f"http://127.0.0.1:{port['FLOW_PRODUCTION_REST_PORT']}",
               "--s3-endpoint", f"http://127.0.0.1:{port['FLOW_PRODUCTION_S3_PORT']}",
               "--binary", str(binaries / "embrasure-flow"),
               "--build-manifest", str(binaries / "build-manifest.json"),
               "--artifacts", str(artifacts), *arguments]
    with open(f"{artifacts}.log", "wb") as log:
        code = subprocess.run(command, cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT).returncode
    return code


def prune(artifacts):
    """Drop the run's RocksDB, journal and spool copies; keep logs, report and metrics."""
    state = artifacts / "state"
    if state.is_dir():
        for entry in state.iterdir():
            if entry.name not in KEEP_STATE:
                shutil.rmtree(entry) if entry.is_dir() else entry.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default="origin/main", help="git revision, or . for this working tree")
    parser.add_argument("--candidate", default=".", help="git revision, or . for this working tree")
    parser.add_argument("--runs", type=int, default=2, help="runs per side")
    parser.add_argument("--profile", choices=("bench-fast", "profiling", "release"), default="bench-fast")
    parser.add_argument("--features", default=DEFAULT_FEATURES,
                        help=f"comma-separated cargo features for both sides (default: {DEFAULT_FEATURES})")
    parser.add_argument("--base-features", help="features for the base side instead, e.g. \"\" to compare allocators")
    parser.add_argument("--args", default=DEFAULT_ARGS, help=f"benchmark.py arguments for both sides (default: {DEFAULT_ARGS})")
    parser.add_argument("--base-args", default="", help="extra benchmark.py arguments for the base side")
    parser.add_argument("--candidate-args", default="", help="extra benchmark.py arguments for the candidate side")
    parser.add_argument("--artifacts", type=Path, help="output directory (default target/ab/runs/<time>)")
    parser.add_argument("--keep-state", action="store_true", help="keep each run's state directory")
    parser.add_argument("--reader-compare", action="store_true",
                        help="also compare each final layout with an independently compacted copy, "
                             "using each side's own local_compactor")
    parser.add_argument("--warm", metavar="REVISION", help="only build REVISION, to fill the build cache")
    args = parser.parse_args()
    def split(value):
        return [feature for feature in value.split(",") if feature]
    features = {side: split(args.features) for side in ("base", "candidate")}
    if args.base_features is not None:
        features["base"] = split(args.base_features)
    if args.warm:
        print(build(source_tree(args.warm), args.profile, features["candidate"]))
        return

    artifacts = (args.artifacts or AB / "runs" / datetime.datetime.now().strftime("%Y%m%d-%H%M%S")).resolve()
    if artifacts.exists():
        parser.error(f"artifacts already exist: {artifacts}")
    sides = {}
    for side, revision in (("base", args.base), ("candidate", args.candidate)):
        sides[side] = build(source_tree(revision), args.profile, features[side])
    fixture("status")
    free = shutil.disk_usage(ROOT).free / 2**30
    if free < 15:
        print(f"warning: {free:.0f} GiB free; runs retain data in the fixture volumes. "
              "Reclaim it with tests/production/fixture.sh reset", flush=True)
    artifacts.mkdir(parents=True)
    common = shlex.split(args.args)
    extra = {"base": shlex.split(args.base_args), "candidate": shlex.split(args.candidate_args)}
    order = []
    for index in range(args.runs):
        pair = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
        order += [(side, index + 1) for side in pair]
    results = {"base": [], "candidate": []}
    for side, index in order:
        fixture("gc-slots")
        directory = artifacts / f"{side}-{index}"
        print(f"[{datetime.datetime.now():%H:%M:%S}] {side} run {index}", flush=True)
        reader = (["--reader-baseline-compactor", str(sides[side] / "local_compactor")]
                  if args.reader_compare else [])
        code = run(sides[side], directory, common + extra[side] + reader)
        if not (directory / "report.json").exists():
            raise SystemExit(f"{side} run {index} produced no report (exit {code}); see {directory}.log")
        row = summarize(directory)
        results[side].append(row)
        if not row["correct"]:
            print(f"  correctness FAILED: {row['failure']}", flush=True)
        if not args.keep_state:
            prune(directory)

    summary = "\n\n".join((
        f"# A/B: {args.base} → {args.candidate} ({args.profile} profile, "
        f"features `{args.features}`, {args.runs} runs per side)"
        + (f"; base features `{args.base_features}`" if args.base_features is not None else ""),
        f"Common arguments: `{args.args}`"
        + (f"; base `{args.base_args}`" if args.base_args else "")
        + (f"; candidate `{args.candidate_args}`" if args.candidate_args else ""),
        f"Base binaries: `{sides['base']}`\n\nCandidate binaries: `{sides['candidate']}`",
        table(results["base"] + results["candidate"]),
        compare(results["base"], results["candidate"]),
    ))
    (artifacts / "summary.md").write_text(summary + "\n")
    print(summary)
    if not all(row["correct"] for rows in results.values() for row in rows):
        raise SystemExit("at least one run failed correctness; see the reports")


if __name__ == "__main__":
    main()
