#!/usr/bin/env python3
"""Check reviewed license terms and package locked daemon notices, without compiling."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tomllib


ROOT = Path(__file__).resolve().parents[1]
LEGAL_NAMES = ("LICENSE", "COPYING", "NOTICE", "COPYRIGHT", "AUTHORS", "UNLICENSE")
# These libraries compile bundled native code whose notices are below crate root.
# RocksDB selects Apache-2.0; LZ4 and Zstandard select their BSD library licenses.
NATIVE_FILES = {
    "librocksdb-sys": [
        "rocksdb/LICENSE.Apache", "rocksdb/LICENSE.leveldb", "rocksdb/AUTHORS",
        "rocksdb/utilities/transactions/lock/range/range_tree/lib/COPYING.APACHEv2",
        "snappy/COPYING", "snappy/AUTHORS",
    ],
    "lz4-sys": ["liblz4/lib/LICENSE"],
    "zstd-sys": ["zstd/LICENSE"],
    "libz-sys": ["src/zlib/LICENSE", "src/zlib-ng/LICENSE.md"],
    "bzip2-sys": ["bzip2-1.0.8/LICENSE"],
    "ring": [
        "third_party/fiat/LICENSE", "src/polyfill/once_cell/LICENSE-APACHE",
        "src/polyfill/once_cell/LICENSE-MIT",
    ],
    "aws-lc-sys": ["aws-lc/LICENSE", "aws-lc/third_party/fiat/LICENSE"],
    "tikv-jemalloc-sys": ["jemalloc/COPYING"],
}
NATIVE_HEADERS = {
    "librocksdb-sys": ["rocksdb/util/xxhash.cc", "rocksdb/util/xxhash.h"],
    "zstd-sys": ["zstd/lib/common/xxhash.h"],
}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def capture(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def copy_notice(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)


def packages_for_daemon(metadata):
    packages = {p["id"]: p for p in metadata["packages"]}
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    pending = [p["id"] for p in packages.values() if p["name"] == "flow-daemon"]
    if len(pending) != 1:
        raise ValueError("expected exactly one flow-daemon package")
    selected = set()
    while pending:
        package_id = pending.pop()
        if package_id in selected:
            continue
        selected.add(package_id)
        pending.extend(
            dep["pkg"] for dep in nodes[package_id]["deps"]
            if any(kind["kind"] != "dev" for kind in dep["dep_kinds"])
        )
    return sorted((packages[p] for p in selected), key=lambda p: (p["name"], p["version"]))


def check_policy(packages, policy):
    """Match reviewed expressions verbatim; this is not an SPDX parser."""
    for p in packages:
        identity = f"{p['name']} {p['version']}"
        if p["license"] not in policy["reviewed_expressions"]:
            raise ValueError(f"unreviewed license expression: {identity}: {p['license']!r}")
        native = policy["bundled_native"].get(p["name"])
        if p["name"] in NATIVE_FILES and native is None:
            raise ValueError(f"unreviewed bundled native component: {identity}")
        review = native or policy["other_links"].get(p["name"])
        if p.get("links") or native:
            if (review is None or review["version"] != p["version"]
                    or review["links"] != p.get("links")):
                raise ValueError(f"unreviewed native linkage: {identity}: {p.get('links')!r}")
        if native:
            source = Path(p["manifest_path"]).parent
            required = set(NATIVE_FILES.get(p["name"], []) + NATIVE_HEADERS.get(p["name"], []))
            if not required.issubset(native["files"]):
                raise ValueError(f"native notice policy is incomplete: {identity}")
            for relative, expected in native["files"].items():
                if sha256((source / relative).read_bytes()) != expected:
                    raise ValueError(f"changed reviewed native notice: {identity}/{relative}")


def metadata_command(target, features, no_default_features):
    command = ["cargo", "metadata", "--locked", "--offline", "--format-version", "1",
               "--filter-platform", target]
    if features:
        command += ["--features", ",".join(
            feature if "/" in feature else f"flow-daemon/{feature}" for feature in features)]
    if no_default_features:
        command.append("--no-default-features")
    return command


def package(args):
    rustc = capture("rustc", "-vV")
    target = args.target or next(line[6:] for line in rustc.splitlines() if line.startswith("host: "))
    features = sorted({feature for group in args.features for feature in re.split(r"[\s,]+", group) if feature})
    metadata = json.loads(capture(*metadata_command(target, features, args.no_default_features)))
    resolved_features = {node["id"]: node["features"] for node in metadata["resolve"]["nodes"]}
    policy_bytes = (ROOT / "licenses/policy.json").read_bytes()
    policy = json.loads(policy_bytes)
    if target not in policy["targets"]:
        raise ValueError(f"unreviewed distribution target: {target}")
    packages = packages_for_daemon(metadata)
    check_policy(packages, policy)
    supplements = json.loads((ROOT / "licenses/supplemental.json").read_text())["supplements"]
    for entry in supplements:
        if sha256((ROOT / "licenses" / entry["file"]).read_bytes()) != entry["sha256"]:
            raise ValueError(f"changed supplemental notice: {entry['file']}")
    if args.check_only:
        print(f"License policy passed: {len(packages)} packages for {target}")
        return
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"output must be absent or empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    for name in ("LICENSE", "NOTICE"):
        copy_notice(ROOT / name, output / name)
    copy_notice(ROOT / "licenses/README.md", output / "README.md")
    copy_notice(ROOT / "licenses/policy.json", output / "policy.json")
    lock_bytes = (ROOT / "Cargo.lock").read_bytes()
    locked = {(p["name"], p["version"]): p for p in tomllib.loads(lock_bytes.decode())["package"]}
    inventory = []
    for p in packages:
        name, version = p["name"], p["version"]
        source = Path(p["manifest_path"]).parent
        destination = output / "third-party" / f"{name}-{version}"
        files = [f for f in source.iterdir() if f.is_file() and f.name.upper().startswith(LEGAL_NAMES)]
        if p["license_file"]:
            files.append(source / p["license_file"])
        for file in sorted(set(files)):
            copy_notice(file, destination / file.name)
        if source.is_relative_to(ROOT / "crates"):
            copy_notice(ROOT / "LICENSE", destination / "LICENSE")
        for relative in NATIVE_FILES.get(name, []):
            copy_notice(source / relative, destination / "native" / relative)
        for relative in NATIVE_HEADERS.get(name, []):
            text = (source / relative).read_text()
            headers = [s for s in re.findall(r"/\*.*?\*/", text, re.DOTALL)
                       if "copyright" in s.lower() and "license" in s.lower()]
            if not headers:
                raise ValueError(f"native license header changed: {name}/{relative}")
            file = destination / "native" / (relative + ".LICENSE")
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("\n\n".join(headers) + "\n")
        additions = [s for s in supplements if (s["name"], s["version"]) == (name, version)]
        for entry in additions:
            copy_notice(ROOT / "licenses" / entry["file"], destination / "supplemental" / Path(entry["file"]).name)
        if not any(file.is_file() and file.name.upper().startswith(("LICENSE", "COPYING", "UNLICENSE"))
                   for file in destination.rglob("*")):
            raise ValueError(f"no license text for {name} {version}; add a pinned supplemental notice")
        if source.is_relative_to(ROOT / "vendor"):
            for relative in (".cargo_vcs_info.json", "LOCAL_CHANGES.md"):
                copy_notice(source / relative, destination / relative)
        inventory.append({
            "name": name, "version": version, "license": p["license"],
            "features": resolved_features[p["id"]],
            "reviewed_license_choice": policy["reviewed_expressions"][p["license"]],
            "repository": p["repository"], "source": p["source"] or str(source.relative_to(ROOT)),
            "crate_sha256": locked[(name, version)].get("checksum"),
            "supplemental_sources": additions,
        })
    # The standard library is linked outside Cargo's package graph. The toolchain
    # ships a generated copyright report that includes its third-party licenses.
    rust_docs = Path(capture("rustc", "--print", "sysroot")) / "share/doc/rust"
    copy_notice(rust_docs / "COPYRIGHT-library.html", output / "rust/COPYRIGHT-library.html")
    for file in sorted((rust_docs / "licenses").glob("*.txt")):
        copy_notice(file, output / "rust/licenses" / file.name)
    if not (output / "rust/licenses/Apache-2.0.txt").exists():
        raise ValueError("Rust toolchain license files are missing")
    inventory_document = {
        "format_version": 1, "target": target, "rustc": rustc,
        "requested_features": features, "no_default_features": args.no_default_features,
        "license_policy_sha256": sha256(policy_bytes),
        "cargo_lock_sha256": sha256(lock_bytes), "packages": inventory,
        "files": {
            str(file.relative_to(output)): sha256(file.read_bytes())
            for file in sorted(output.rglob("*")) if file.is_file()
        },
    }
    (output / "index.json").write_text(json.dumps(inventory_document, indent=2, sort_keys=True) + "\n")
    print(f"Packaged {len(inventory)} dependency notices for {target} in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--output", type=Path)
    action.add_argument("--check-only", action="store_true", help="check policy without writing a bundle")
    parser.add_argument("--target", help="Cargo target triple; defaults to rustc host")
    parser.add_argument("--features", action="append", default=[],
                        help="enabled daemon features, matching the build (repeatable; comma/space separated)")
    parser.add_argument("--no-default-features", action="store_true", help="match a build without default features")
    package(parser.parse_args())
